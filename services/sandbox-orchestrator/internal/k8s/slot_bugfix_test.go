// Package k8s defines tests for slot bugfix test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package k8s

import (
	"context"
	"errors"
	"testing"

	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/sandbox-orchestrator/internal/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// bugfixManager performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func bugfixManager(captureEnabled bool) *Manager {
	return &Manager{
		client:         fake.NewSimpleClientset(),
		namespace:      "sandbox",
		cpu:            "2",
		memory:         "1Gi",
		captureEnabled: captureEnabled,
		captureImage:   "capture:dev",
		kafkaBrokers:   "kafka:9092",
	}
}

// TestAlgoPodHardened performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAlgoPodHardened(t *testing.T) {
	sc := bugfixManager(false).podSpec("s1", "c1", "img", []int{9898}, topics.OrderBandUnset).Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("algo container has no SecurityContext")
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Error("AllowPrivilegeEscalation must be false")
	}
	if sc.Capabilities == nil || len(sc.Capabilities.Drop) == 0 || sc.Capabilities.Drop[0] != "ALL" {
		t.Errorf("capabilities must drop ALL, got %+v", sc.Capabilities)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("seccomp profile must be RuntimeDefault")
	}
	if sc.RunAsNonRoot != nil && *sc.RunAsNonRoot {
		t.Error("must NOT force RunAsNonRoot on arbitrary contestant images")
	}
}

// TestAlgoPodHasActiveDeadline performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAlgoPodHasActiveDeadline(t *testing.T) {
	pod := bugfixManager(false).podSpec("s1", "c1", "img", []int{9898}, topics.OrderBandUnset)
	if pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds <= 0 {
		t.Fatalf("algo pod missing ActiveDeadlineSeconds backstop: %v", pod.Spec.ActiveDeadlineSeconds)
	}
}

// TestCreateSlotRejectsUncapturablePort performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCreateSlotRejectsUncapturablePort(t *testing.T) {
	ctx := context.Background()
	m := bugfixManager(true)
	if err := m.CreateSlot(ctx, "s-bad", "c1", "img", []int{1234}, topics.OrderBandUnset); !errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("port 1234 (capture on): got %v, want ErrInvalidRequest", err)
	}
	if err := m.CreateSlot(ctx, "s-ok", "c1", "img", []int{9898}, topics.OrderBandUnset); errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("port 9898 (capture on) wrongly rejected: %v", err)
	}
	if err := bugfixManager(false).CreateSlot(ctx, "s-any", "c1", "img", []int{1234}, topics.OrderBandUnset); errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("port 1234 (capture off) wrongly rejected: %v", err)
	}
}

// TestCaptureJobBoundedAndOwned performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCaptureJobBoundedAndOwned(t *testing.T) {
	job := bugfixManager(true).captureJobSpec("s1", "c1", "node-1", "pod-uid-123", "containerd://abc", topics.OrderBandUnset)
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds <= 0 {
		t.Error("capture Job missing ActiveDeadlineSeconds")
	}
	if len(job.OwnerReferences) == 0 {
		t.Fatal("capture Job missing ownerReference to the algo pod")
	}
	ref := job.OwnerReferences[0]
	if ref.Kind != "Pod" || string(ref.UID) != "pod-uid-123" {
		t.Errorf("ownerReference = %+v, want Pod/pod-uid-123", ref)
	}
}
