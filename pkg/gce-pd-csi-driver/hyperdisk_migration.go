/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gceGCEDriver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/GoogleCloudPlatform/k8s-cloud-provider/pkg/cloud/meta"
	computev1 "google.golang.org/api/compute/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
)

// Hyperdisk migration converts PD disks (pd-balanced, pd-standard, pd-ssd) to
// hyperdisk-balanced using a durable, crash-recoverable state machine driven
// entirely from observable GCE resource state. See plans/woolly-jingling-rose.md
// for the design.
//
// Deterministic per-disk artifact names ensure every controller replica can
// resume a migration without sidechannel state.
const (
	hyperdiskSnapshotSuffix = "-hdm-snap"
	hyperdiskTempSuffix     = "-hdm-tmp"
	gceResourceNameMaxLen   = 63

	hyperdiskBalancedType = "hyperdisk-balanced"
	pdBalancedType        = "pd-balanced"
	pdStandardType        = "pd-standard"
	pdSSDType             = "pd-ssd"

	labelHyperdiskMigrationSourceName = "hyperdisk-migration_csi_storage_gke_io_source-disk-name"
	labelHyperdiskMigrationTargetType = "hyperdisk-migration_csi_storage_gke_io_target-disk-type"
)

func hyperdiskSnapshotName(origDisk string) string {
	return hyperdiskMigrationResourceName(origDisk, hyperdiskSnapshotSuffix)
}

func hyperdiskTempDiskName(origDisk string) string {
	return hyperdiskMigrationResourceName(origDisk, hyperdiskTempSuffix)
}

func hyperdiskMigrationResourceName(origDisk, suffix string) string {
	if len(origDisk)+len(suffix) <= gceResourceNameMaxLen {
		return origDisk + suffix
	}
	hash := common.ShortString(origDisk)
	maxPrefixLen := gceResourceNameMaxLen - len(suffix) - len(hash) - 1
	return origDisk[:maxPrefixLen] + "-" + hash + suffix
}

// maybeRecoverMigration drives the state machine to completion whenever an
// in-flight migration snapshot exists, regardless of whether the source disk
// is still present. It runs BEFORE the normal GetDisk/NotFound handling so a
// crash between source-delete and final-insert does not become terminal.
func (gceCS *GCEControllerServer) maybeRecoverMigration(ctx context.Context, project string, volKey *meta.Key) error {
	if !gceCS.enableHyperdiskMigration {
		return nil
	}
	if volKey.Type() != meta.Zonal {
		return nil
	}
	snapName := hyperdiskSnapshotName(volKey.Name)
	snap, err := gceCS.CloudProvider.GetSnapshotOrNil(ctx, project, snapName)
	if err != nil {
		return status.Errorf(codes.Internal, "hyperdisk-migration: check snapshot %s: %v", snapName, err)
	}
	if snap == nil {
		return nil
	}
	return gceCS.runConvertStepWithLock(ctx, project, volKey)
}

// maybeInitiateMigration starts a fresh migration when the target node's
// machine family requires hyperdisk-balanced and the disk is currently
// a supported PD type. After it fires once, subsequent retries are serviced
// by maybeRecoverMigration.
func (gceCS *GCEControllerServer) maybeInitiateMigration(ctx context.Context, project string, volKey *meta.Key, disk *gce.CloudDisk, instance *computev1.Instance) error {
	if !gceCS.enableHyperdiskMigration {
		return nil
	}
	if instance == nil || disk == nil {
		return nil
	}
	if volKey.Type() != meta.Zonal {
		return nil
	}
	family := common.MachineFamilyFromType(instance.MachineType)
	if family == "" || !gceCS.hyperdiskRequiredFamilies.Has(family) {
		return nil
	}
	current := disk.GetPDType()
	if current == hyperdiskBalancedType {
		return nil
	}
	if !isSupportedHyperdiskMigrationSourceType(current) {
		// Only selected zonal PD types are supported in v1. Log and fall through
		// to the existing attach path so mismatches surface as the usual
		// UnsupportedDiskError.
		klog.V(4).Infof("hyperdisk-migration: skipping disk %s with unsupported current type %q (target family %s)", volKey.Name, current, family)
		return nil
	}
	return gceCS.runConvertStepWithLock(ctx, project, volKey)
}

func isSupportedHyperdiskMigrationSourceType(diskType string) bool {
	switch diskType {
	case pdBalancedType, pdStandardType, pdSSDType:
		return true
	default:
		return false
	}
}

func (gceCS *GCEControllerServer) runConvertStepWithLock(ctx context.Context, project string, volKey *meta.Key) error {
	lockKey := "hdm/" + volKey.String()
	if !gceCS.convertLocks.TryAcquire(lockKey) {
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: another step in flight for disk %s", volKey.Name)
	}
	defer gceCS.convertLocks.Release(lockKey)
	return gceCS.runConvertStep(ctx, project, volKey)
}

// runConvertStep is the idempotent state machine. Each invocation re-observes
// the three resources (source disk, snapshot, temp disk), issues at most one
// advancing RPC, and returns codes.Unavailable so kubelet retries.
//
// INVARIANT: DELETE source is only issued after observing temp.Status=READY in
// THIS call. The switch-arm ordering below is load-bearing; do not reorder.
func (gceCS *GCEControllerServer) runConvertStep(ctx context.Context, project string, volKey *meta.Key) error {
	snapName := hyperdiskSnapshotName(volKey.Name)
	tempKey := meta.ZonalKey(hyperdiskTempDiskName(volKey.Name), volKey.Zone)

	source, err := gceCS.CloudProvider.GetDiskOrNil(ctx, project, volKey)
	if err != nil {
		return status.Errorf(codes.Internal, "hyperdisk-migration: get source %s: %v", volKey.Name, err)
	}
	snap, err := gceCS.CloudProvider.GetSnapshotOrNil(ctx, project, snapName)
	if err != nil {
		return status.Errorf(codes.Internal, "hyperdisk-migration: get snapshot %s: %v", snapName, err)
	}
	temp, err := gceCS.CloudProvider.GetDiskOrNil(ctx, project, tempKey)
	if err != nil {
		return status.Errorf(codes.Internal, "hyperdisk-migration: get temp %s: %v", tempKey.Name, err)
	}

	switch {
	case snap == nil && source == nil:
		// Neither side exists; nothing safe to do. Fall through so the caller
		// produces its normal NotFound error.
		return nil

	case snap == nil:
		// Step 1: snapshot the source. Only reachable from the initiation gate,
		// which has already verified source is a supported PD type.
		labels := buildMigrationLabels(volKey.Name)
		description := encodeSourceLabelsDescription(source.GetLabels())
		if err := gceCS.CloudProvider.SnapshotDisk(ctx, project, volKey, snapName, labels, description); err != nil {
			return status.Errorf(codes.Internal, "hyperdisk-migration: snapshot %s: %v", snapName, err)
		}
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: snapshot started", volKey.Name)

	case snap.Status != "READY":
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: snapshot %s status=%s", volKey.Name, snapName, snap.Status)

	case temp == nil:
		// Step 2: create temp disk with target type, from the snapshot. Use the
		// snapshot's DiskSizeGb; authoritative even if source is already gone.
		if err := gceCS.CloudProvider.InsertDiskFromSnapshot(ctx, project, tempKey, snap.SelfLink, hyperdiskBalancedType, snap.DiskSizeGb, buildMigrationLabels(volKey.Name)); err != nil {
			return status.Errorf(codes.Internal, "hyperdisk-migration: create temp %s: %v", tempKey.Name, err)
		}
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: temp provisioning", volKey.Name)

	case temp.GetStatus() != "READY":
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: temp %s status=%s", volKey.Name, tempKey.Name, temp.GetStatus())

	// INVARIANT: reaching the source-delete branches below requires falling
	// past temp == nil AND temp.Status != READY above. Do not reorder.
	case source != nil && source.GetPDType() == hyperdiskBalancedType && source.GetStatus() != "READY":
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: final status=%s", volKey.Name, source.GetStatus())

	case source != nil && source.GetPDType() == hyperdiskBalancedType:
		// The final disk has already been recreated at the original name. Clean
		// up artifacts instead of treating it as the old source and deleting it.
		if err := gceCS.cleanupMigrationArtifacts(ctx, project, volKey); err != nil {
			return err
		}
		return nil

	case source != nil && source.GetStatus() == "DELETING":
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: source status=DELETING", volKey.Name)

	case source != nil:
		// Step 3: delete source. DeleteDisk is idempotent on notFound.
		if err := gceCS.CloudProvider.DeleteDisk(ctx, project, volKey); err != nil && !gce.IsGCENotFoundError(err) {
			return status.Errorf(codes.Internal, "hyperdisk-migration: delete source %s: %v", volKey.Name, err)
		}
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: source delete issued", volKey.Name)
	}

	// source == nil, snap.READY, temp.READY.
	// Step 4: re-observe the original-name slot and create the final disk from
	// temp if it is missing. Re-Get to avoid basing the decision on the stale
	// `source` variable.
	final, err := gceCS.CloudProvider.GetDiskOrNil(ctx, project, volKey)
	if err != nil {
		return status.Errorf(codes.Internal, "hyperdisk-migration: get final %s: %v", volKey.Name, err)
	}
	switch {
	case final == nil:
		origLabels := readSourceLabelsFromSnapshot(snap)
		if err := gceCS.CloudProvider.InsertDiskFromDisk(ctx, project, volKey, temp.GetSelfLink(), origLabels); err != nil {
			return status.Errorf(codes.Internal, "hyperdisk-migration: create final %s: %v", volKey.Name, err)
		}
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: final provisioning", volKey.Name)

	case final.GetStatus() != "READY":
		return status.Errorf(codes.Unavailable, "hyperdisk-migration: disk %s: final status=%s", volKey.Name, final.GetStatus())

	case final.GetPDType() != hyperdiskBalancedType:
		return status.Errorf(codes.Internal, "hyperdisk-migration: disk %s: final type=%s (expected %s)", volKey.Name, final.GetPDType(), hyperdiskBalancedType)
	}

	// Step 5: cleanup temp + snapshot. Deletes tolerate notFound.
	if err := gceCS.cleanupMigrationArtifacts(ctx, project, volKey); err != nil {
		return err
	}
	return nil
}

func (gceCS *GCEControllerServer) cleanupMigrationArtifacts(ctx context.Context, project string, volKey *meta.Key) error {
	tempKey := meta.ZonalKey(hyperdiskTempDiskName(volKey.Name), volKey.Zone)
	if err := gceCS.CloudProvider.DeleteDisk(ctx, project, tempKey); err != nil && !gce.IsGCENotFoundError(err) {
		return status.Errorf(codes.Internal, "hyperdisk-migration: delete temp %s: %v", tempKey.Name, err)
	}
	snapName := hyperdiskSnapshotName(volKey.Name)
	if err := gceCS.CloudProvider.DeleteSnapshot(ctx, project, snapName); err != nil && !gce.IsGCENotFoundError(err) {
		return status.Errorf(codes.Internal, "hyperdisk-migration: delete snapshot %s: %v", snapName, err)
	}
	klog.V(4).Infof("hyperdisk-migration: cleanup complete for disk %s", volKey.Name)
	return nil
}

// buildMigrationLabels produces the label map written on migration artifacts.
// Keys use underscores rather than dots/slashes because GCE label keys only
// accept [a-z0-9_-].
func buildMigrationLabels(origDiskName string) map[string]string {
	return map[string]string{
		labelHyperdiskMigrationSourceName: origDiskName,
		labelHyperdiskMigrationTargetType: hyperdiskBalancedType,
	}
}

// encodeSourceLabelsDescription packs the source disk's labels into a JSON
// string stored in the snapshot's Description. This sidesteps the 63-char
// label-value limit (JSON of many labels can easily exceed it).
func encodeSourceLabelsDescription(sourceLabels map[string]string) string {
	if len(sourceLabels) == 0 {
		return ""
	}
	b, err := json.Marshal(sourceLabels)
	if err != nil {
		klog.Warningf("hyperdisk-migration: failed to marshal source labels: %v", err)
		return ""
	}
	return fmt.Sprintf("hdm-source-labels:%s", string(b))
}

func readSourceLabelsFromSnapshot(snap *computev1.Snapshot) map[string]string {
	if snap == nil {
		return nil
	}
	const prefix = "hdm-source-labels:"
	desc := snap.Description
	if len(desc) < len(prefix) || desc[:len(prefix)] != prefix {
		return nil
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(desc[len(prefix):]), &labels); err != nil {
		klog.Warningf("hyperdisk-migration: failed to unmarshal source labels from snapshot %s: %v", snap.Name, err)
		return nil
	}
	return labels
}
