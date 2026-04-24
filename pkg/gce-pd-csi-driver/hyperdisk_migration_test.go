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
	"fmt"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/k8s-cloud-provider/pkg/cloud/meta"
	computebeta "google.golang.org/api/compute/v0.beta"
	computev1 "google.golang.org/api/compute/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/sets"

	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
)

const (
	hdmDiskName = "pvc-abc"
)

func hdmVolKey() *meta.Key { return meta.ZonalKey(hdmDiskName, zone) }

// countingFakeCloud wraps FakeCloudProvider to count disk-delete calls. Used
// to enforce the data-safety invariant: DELETE source must not fire until
// temp.Status == READY is observed in the same call.
type countingFakeCloud struct {
	*gce.FakeCloudProvider
	deleteDiskCalls     map[string]int
	deleteSnapshotCalls int
}

func newCountingFakeCloud(t *testing.T, cloudDisks []*gce.CloudDisk) *countingFakeCloud {
	fcp, err := gce.CreateFakeCloudProvider(project, zone, cloudDisks)
	if err != nil {
		t.Fatalf("CreateFakeCloudProvider: %v", err)
	}
	return &countingFakeCloud{
		FakeCloudProvider: fcp,
		deleteDiskCalls:   map[string]int{},
	}
}

func (c *countingFakeCloud) DeleteDisk(ctx context.Context, project string, volKey *meta.Key) error {
	c.deleteDiskCalls[volKey.Name]++
	return c.FakeCloudProvider.DeleteDisk(ctx, project, volKey)
}

func (c *countingFakeCloud) DeleteSnapshot(ctx context.Context, project, name string) error {
	c.deleteSnapshotCalls++
	return c.FakeCloudProvider.DeleteSnapshot(ctx, project, name)
}

func newMigrationControllerServer(cloud gce.GCECompute, enable bool, families ...string) *GCEControllerServer {
	args := &GCEControllerServerArgs{
		EnableHyperdiskMigration:  enable,
		HyperdiskRequiredFamilies: families,
	}
	return controllerServerForTest(cloud, args)
}

func zonalDiskV1(name, diskType, status string, labels map[string]string, sizeGb int64) *gce.CloudDisk {
	return gce.CloudDiskFromV1(&computev1.Disk{
		Name:     name,
		Zone:     zone,
		SizeGb:   sizeGb,
		Type:     "projects/" + project + "/zones/" + zone + "/diskTypes/" + diskType,
		Status:   status,
		Labels:   labels,
		SelfLink: fmt.Sprintf("%sprojects/%s/zones/%s/disks/%s", gce.BasePath, project, zone, name),
	})
}

func zonalDiskBeta(name, diskType, status string, labels map[string]string, sizeGb int64) *gce.CloudDisk {
	return gce.CloudDiskFromBeta(&computebeta.Disk{
		Name:     name,
		Zone:     zone,
		SizeGb:   sizeGb,
		Type:     "projects/" + project + "/zones/" + zone + "/diskTypes/" + diskType,
		Status:   status,
		Labels:   labels,
		SelfLink: fmt.Sprintf("%sprojects/%s/zones/%s/disks/%s", gce.BasePath, project, zone, name),
	})
}

func seedSnapshot(t *testing.T, cs *GCEControllerServer, name, status string) *computev1.Snapshot {
	t.Helper()
	// Use the real cloud provider's SnapshotDisk then override status.
	err := cs.CloudProvider.SnapshotDisk(context.Background(), project, hdmVolKey(), name, buildMigrationLabels(hdmDiskName), "")
	if err != nil {
		t.Fatalf("seed SnapshotDisk: %v", err)
	}
	snap, err := cs.CloudProvider.GetSnapshotOrNil(context.Background(), project, name)
	if err != nil || snap == nil {
		t.Fatalf("seed GetSnapshotOrNil: snap=%v err=%v", snap, err)
	}
	snap.Status = status
	return snap
}

func TestRunConvertStep_FeatureFlagOff(t *testing.T) {
	fcp, _ := gce.CreateFakeCloudProvider(project, zone, nil)
	cs := newMigrationControllerServer(fcp, false, "n4")
	instance := &computev1.Instance{MachineType: "zones/" + zone + "/machineTypes/n4-standard-4"}
	disk := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", nil, 10)
	if err := cs.maybeInitiateMigration(context.Background(), project, hdmVolKey(), disk, instance); err != nil {
		t.Fatalf("expected nil (flag off), got %v", err)
	}
	if err := cs.maybeRecoverMigration(context.Background(), project, hdmVolKey()); err != nil {
		t.Fatalf("expected nil (flag off), got %v", err)
	}
}

func TestMaybeInitiateMigration_WrongFamily(t *testing.T) {
	fcp, _ := gce.CreateFakeCloudProvider(project, zone, nil)
	cs := newMigrationControllerServer(fcp, true, "n4")
	instance := &computev1.Instance{MachineType: "zones/" + zone + "/machineTypes/n2-standard-4"}
	disk := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", nil, 10)
	if err := cs.maybeInitiateMigration(context.Background(), project, hdmVolKey(), disk, instance); err != nil {
		t.Fatalf("expected nil for non-required family, got %v", err)
	}
	if snap, _ := fcp.GetSnapshotOrNil(context.Background(), project, hyperdiskSnapshotName(hdmDiskName)); snap != nil {
		t.Fatalf("no snapshot should have been created, got %+v", snap)
	}
}

func TestMaybeInitiateMigration_AlreadyTargetType(t *testing.T) {
	fcp, _ := gce.CreateFakeCloudProvider(project, zone, nil)
	cs := newMigrationControllerServer(fcp, true, "n4")
	instance := &computev1.Instance{MachineType: "zones/" + zone + "/machineTypes/n4-standard-4"}
	disk := zonalDiskV1(hdmDiskName, "hyperdisk-balanced", "READY", nil, 10)
	if err := cs.maybeInitiateMigration(context.Background(), project, hdmVolKey(), disk, instance); err != nil {
		t.Fatalf("expected nil when disk already hyperdisk-balanced, got %v", err)
	}
	if snap, _ := fcp.GetSnapshotOrNil(context.Background(), project, hyperdiskSnapshotName(hdmDiskName)); snap != nil {
		t.Fatalf("no snapshot should have been created, got %+v", snap)
	}
}

func TestMaybeInitiateMigration_SupportedSourceTypes(t *testing.T) {
	for _, diskType := range []string{pdBalancedType, pdStandardType, pdSSDType} {
		t.Run(diskType, func(t *testing.T) {
			disk := zonalDiskV1(hdmDiskName, diskType, "READY", nil, 10)
			fcp, _ := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{disk})
			cs := newMigrationControllerServer(fcp, true, "n4")
			instance := &computev1.Instance{MachineType: "zones/" + zone + "/machineTypes/n4-standard-4"}
			err := cs.maybeInitiateMigration(context.Background(), project, hdmVolKey(), disk, instance)
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("expected migration start for %s, got %v", diskType, err)
			}
			if snap, _ := fcp.GetSnapshotOrNil(context.Background(), project, hyperdiskSnapshotName(hdmDiskName)); snap == nil {
				t.Fatalf("snapshot should have been created for %s", diskType)
			}
		})
	}
}

func TestMaybeInitiateMigration_UnsupportedCurrentType(t *testing.T) {
	// Other source types should be a no-op (falls through to existing attach path).
	fcp, _ := gce.CreateFakeCloudProvider(project, zone, nil)
	cs := newMigrationControllerServer(fcp, true, "n4")
	instance := &computev1.Instance{MachineType: "zones/" + zone + "/machineTypes/n4-standard-4"}
	disk := zonalDiskV1(hdmDiskName, "pd-extreme", "READY", nil, 10)
	if err := cs.maybeInitiateMigration(context.Background(), project, hdmVolKey(), disk, instance); err != nil {
		t.Fatalf("expected nil for unsupported source type, got %v", err)
	}
	if snap, _ := fcp.GetSnapshotOrNil(context.Background(), project, hyperdiskSnapshotName(hdmDiskName)); snap != nil {
		t.Fatalf("no snapshot should have been created for pd-extreme, got %+v", snap)
	}
}

// Step 1: no snapshot yet, so snapshot is issued and error is Unavailable.
func TestRunConvertStep_StartsSnapshot(t *testing.T) {
	sourceLabels := map[string]string{"owner": "team-a"}
	source := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", sourceLabels, 10)
	fcp, _ := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{source})
	cs := newMigrationControllerServer(fcp, true, "n4")

	err := cs.runConvertStep(context.Background(), project, hdmVolKey())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	snap, err := fcp.GetSnapshotOrNil(context.Background(), project, hyperdiskSnapshotName(hdmDiskName))
	if err != nil || snap == nil {
		t.Fatalf("snapshot not created: snap=%v err=%v", snap, err)
	}
	if snap.Labels[labelHyperdiskMigrationSourceName] != hdmDiskName {
		t.Errorf("missing source-disk-name label: %v", snap.Labels)
	}
	if !strings.Contains(snap.Description, "team-a") {
		t.Errorf("source labels not encoded in description: %q", snap.Description)
	}
}

// Step 2 gate: snapshot still CREATING means Unavailable and no temp disk.
func TestRunConvertStep_SnapshotStillCreating(t *testing.T) {
	source := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", nil, 10)
	cc := newCountingFakeCloud(t, []*gce.CloudDisk{source})
	cs := newMigrationControllerServer(cc, true, "n4")
	seedSnapshot(t, cs, hyperdiskSnapshotName(hdmDiskName), "CREATING")

	err := cs.runConvertStep(context.Background(), project, hdmVolKey())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while snapshot CREATING, got %v", err)
	}
	if temp, _ := cc.GetDiskOrNil(context.Background(), project, meta.ZonalKey(hyperdiskTempDiskName(hdmDiskName), zone)); temp != nil {
		t.Fatalf("temp disk should not exist while snap is CREATING, got %+v", temp)
	}
	if cc.deleteDiskCalls[hdmDiskName] != 0 {
		t.Fatalf("source MUST NOT be deleted yet; got %d deletes", cc.deleteDiskCalls[hdmDiskName])
	}
}

// Step 2: snapshot READY and temp missing means temp insert is issued.
func TestRunConvertStep_CreatesTempFromSnapshot(t *testing.T) {
	source := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", nil, 10)
	cc := newCountingFakeCloud(t, []*gce.CloudDisk{source})
	cs := newMigrationControllerServer(cc, true, "n4")
	seedSnapshot(t, cs, hyperdiskSnapshotName(hdmDiskName), "READY")

	err := cs.runConvertStep(context.Background(), project, hdmVolKey())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	temp, _ := cc.GetDiskOrNil(context.Background(), project, meta.ZonalKey(hyperdiskTempDiskName(hdmDiskName), zone))
	if temp == nil {
		t.Fatalf("temp disk should have been created")
	}
	if temp.GetPDType() != hyperdiskBalancedType {
		t.Fatalf("temp type=%s, want %s", temp.GetPDType(), hyperdiskBalancedType)
	}
	if cc.deleteDiskCalls[hdmDiskName] != 0 {
		t.Fatalf("source MUST NOT be deleted yet; got %d deletes", cc.deleteDiskCalls[hdmDiskName])
	}
}

// INVARIANT TEST: source must not be deleted while temp is still CREATING.
func TestRunConvertStep_SourceNotDeletedWhileTempCreating(t *testing.T) {
	source := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", nil, 10)
	// Pre-seed temp disk in CREATING state.
	tempCreating := zonalDiskBeta(hyperdiskTempDiskName(hdmDiskName), hyperdiskBalancedType, "CREATING", nil, 10)
	cc := newCountingFakeCloud(t, []*gce.CloudDisk{source, tempCreating})
	cs := newMigrationControllerServer(cc, true, "n4")
	seedSnapshot(t, cs, hyperdiskSnapshotName(hdmDiskName), "READY")

	err := cs.runConvertStep(context.Background(), project, hdmVolKey())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while temp CREATING, got %v", err)
	}
	if cc.deleteDiskCalls[hdmDiskName] != 0 {
		t.Fatalf("DATA SAFETY INVARIANT: source deleted while temp CREATING; got %d deletes", cc.deleteDiskCalls[hdmDiskName])
	}
}

// Step 3: temp READY and source still present means source delete is issued.
func TestRunConvertStep_DeletesSourceAfterTempReady(t *testing.T) {
	source := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", nil, 10)
	tempReady := zonalDiskBeta(hyperdiskTempDiskName(hdmDiskName), hyperdiskBalancedType, "READY", nil, 10)
	cc := newCountingFakeCloud(t, []*gce.CloudDisk{source, tempReady})
	cs := newMigrationControllerServer(cc, true, "n4")
	seedSnapshot(t, cs, hyperdiskSnapshotName(hdmDiskName), "READY")

	err := cs.runConvertStep(context.Background(), project, hdmVolKey())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable after source delete, got %v", err)
	}
	if cc.deleteDiskCalls[hdmDiskName] != 1 {
		t.Fatalf("source delete count=%d, want 1", cc.deleteDiskCalls[hdmDiskName])
	}
	if d, _ := cc.GetDiskOrNil(context.Background(), project, hdmVolKey()); d != nil {
		t.Fatalf("source disk should be gone, got %+v", d)
	}
}

// Step 4: source gone and temp READY means final disk insert is issued at the orig name.
// Exercises the recovery entry point with the source already deleted; the
// failure mode that motivated the split-gate design.
func TestRunConvertStep_RecoversAfterSourceDeleted(t *testing.T) {
	// No source disk; it was deleted in a previous call.
	tempReady := zonalDiskBeta(hyperdiskTempDiskName(hdmDiskName), hyperdiskBalancedType, "READY", nil, 10)
	cc := newCountingFakeCloud(t, []*gce.CloudDisk{tempReady})
	cs := newMigrationControllerServer(cc, true, "n4")
	seedSnapshot(t, cs, hyperdiskSnapshotName(hdmDiskName), "READY")

	// Must run as recovery entry point (the initiation gate needs a disk).
	err := cs.maybeRecoverMigration(context.Background(), project, hdmVolKey())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on final insert, got %v", err)
	}
	final, _ := cc.GetDiskOrNil(context.Background(), project, hdmVolKey())
	if final == nil {
		t.Fatalf("final disk should have been created")
	}
	if final.GetPDType() != hyperdiskBalancedType {
		t.Fatalf("final type=%s, want %s", final.GetPDType(), hyperdiskBalancedType)
	}
}

// Step 5: final READY with target type and temp/snap still present means cleanup runs.
func TestRunConvertStep_CleansUpWhenComplete(t *testing.T) {
	final := zonalDiskV1(hdmDiskName, hyperdiskBalancedType, "READY", nil, 10)
	tempReady := zonalDiskBeta(hyperdiskTempDiskName(hdmDiskName), hyperdiskBalancedType, "READY", nil, 10)
	cc := newCountingFakeCloud(t, []*gce.CloudDisk{final, tempReady})
	cs := newMigrationControllerServer(cc, true, "n4")
	seedSnapshot(t, cs, hyperdiskSnapshotName(hdmDiskName), "READY")

	if err := cs.runConvertStep(context.Background(), project, hdmVolKey()); err != nil {
		t.Fatalf("runConvertStep should clean up completed migration, got %v", err)
	}
	if cc.deleteDiskCalls[hdmDiskName] != 0 {
		t.Fatalf("final disk must not be deleted during cleanup; got %d deletes", cc.deleteDiskCalls[hdmDiskName])
	}
	if cc.deleteDiskCalls[hyperdiskTempDiskName(hdmDiskName)] != 1 {
		t.Fatalf("temp delete count=%d, want 1", cc.deleteDiskCalls[hyperdiskTempDiskName(hdmDiskName)])
	}
	if cc.deleteSnapshotCalls != 1 {
		t.Fatalf("snapshot delete count=%d, want 1", cc.deleteSnapshotCalls)
	}
	if got, _ := cc.GetDiskOrNil(context.Background(), project, hdmVolKey()); got == nil {
		t.Fatalf("final disk should remain after cleanup")
	}
}

// Recovery gate with no snapshot is a no-op.
func TestMaybeRecoverMigration_NoSnapshot(t *testing.T) {
	fcp, _ := gce.CreateFakeCloudProvider(project, zone, nil)
	cs := newMigrationControllerServer(fcp, true, "n4")
	if err := cs.maybeRecoverMigration(context.Background(), project, hdmVolKey()); err != nil {
		t.Fatalf("expected nil when no snapshot, got %v", err)
	}
}

// Ensure the machine-family set is correctly parsed and matched.
func TestMachineFamilyDetection(t *testing.T) {
	fcp, _ := gce.CreateFakeCloudProvider(project, zone, nil)
	cs := newMigrationControllerServer(fcp, true, "n4", "c4a")
	cases := []struct {
		machineType string
		wantStart   bool
	}{
		{"zones/" + zone + "/machineTypes/n4-standard-4", true},
		{"zones/" + zone + "/machineTypes/c4a-highmem-8", true},
		{"zones/" + zone + "/machineTypes/n2-standard-4", false},
		{"zones/" + zone + "/machineTypes/c3-highcpu-16", false},
	}
	for _, tc := range cases {
		t.Run(tc.machineType, func(t *testing.T) {
			disk := zonalDiskV1(hdmDiskName, "pd-balanced", "READY", nil, 10)
			fcp2, _ := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{disk})
			cs2 := newMigrationControllerServer(fcp2, true, "n4", "c4a")
			instance := &computev1.Instance{MachineType: tc.machineType}
			err := cs2.maybeInitiateMigration(context.Background(), project, hdmVolKey(), disk, instance)
			snap, _ := fcp2.GetSnapshotOrNil(context.Background(), project, hyperdiskSnapshotName(hdmDiskName))
			if tc.wantStart {
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("expected migration start (Unavailable), got %v", err)
				}
				if snap == nil {
					t.Fatalf("expected snapshot to be created")
				}
			} else {
				if err != nil {
					t.Fatalf("expected nil, got %v", err)
				}
				if snap != nil {
					t.Fatalf("expected no snapshot, got %+v", snap)
				}
			}
		})
	}
	_ = cs // appease linter when loop short-circuits
}

// Re-read after write: encoded source labels round-trip through the snapshot.
func TestSourceLabelsRoundTrip(t *testing.T) {
	orig := map[string]string{"team": "a", "env": "prod"}
	desc := encodeSourceLabelsDescription(orig)
	snap := &computev1.Snapshot{Description: desc}
	got := readSourceLabelsFromSnapshot(snap)
	if len(got) != len(orig) {
		t.Fatalf("got %d labels, want %d (got=%v)", len(got), len(orig), got)
	}
	for k, v := range orig {
		if got[k] != v {
			t.Errorf("label %q: got %q, want %q", k, got[k], v)
		}
	}
}

func TestSourceLabelsRoundTrip_Empty(t *testing.T) {
	if desc := encodeSourceLabelsDescription(nil); desc != "" {
		t.Fatalf("empty labels should produce empty description, got %q", desc)
	}
	if got := readSourceLabelsFromSnapshot(&computev1.Snapshot{Description: ""}); got != nil {
		t.Fatalf("empty description should produce nil labels, got %v", got)
	}
}

// Ensures the required-families set is compared case-sensitively against the
// parsed family (families are lowercase per GCE convention).
func TestHyperdiskRequiredFamiliesSet(t *testing.T) {
	got := sets.NewString("n4", "c4a", "c4d")
	if !got.Has("n4") || got.Has("N4") {
		t.Fatalf("unexpected set semantics: %v", got.List())
	}
}

func TestHyperdiskMigrationArtifactNamesFitGCENameLimit(t *testing.T) {
	if got := hyperdiskSnapshotName(hdmDiskName); got != hdmDiskName+hyperdiskSnapshotSuffix {
		t.Fatalf("short snapshot name = %q, want %q", got, hdmDiskName+hyperdiskSnapshotSuffix)
	}
	longName := strings.Repeat("a", gceResourceNameMaxLen)
	for _, name := range []string{hyperdiskSnapshotName(longName), hyperdiskTempDiskName(longName)} {
		if len(name) > gceResourceNameMaxLen {
			t.Fatalf("artifact name %q has length %d, want <= %d", name, len(name), gceResourceNameMaxLen)
		}
		if !strings.HasPrefix(name, "a") {
			t.Fatalf("artifact name should retain source prefix, got %q", name)
		}
	}
}
