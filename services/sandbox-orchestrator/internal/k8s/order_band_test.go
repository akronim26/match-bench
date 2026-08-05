// Package k8s defines tests for ORDER_BAND propagation onto the capture
// container (bot-fleet-controller's exclusive per-session order-band lease).
package k8s

import (
	"strconv"
	"testing"

	"github.com/iicpc/schemas/topics"
	corev1 "k8s.io/api/core/v1"
)

func TestPodSpecStampsOrderBandAnnotation(t *testing.T) {
	pod := bugfixManager(false).podSpec("s1", "c1", "img", []int{9898}, 5)
	if got := pod.Annotations[captureOrderBandAnnotation]; got != "5" {
		t.Fatalf("order band annotation = %q, want %q", got, "5")
	}
}

func TestPodSpecStampsOrderBandAnnotationUnsetSentinel(t *testing.T) {
	pod := bugfixManager(false).podSpec("s1", "c1", "img", []int{9898}, topics.OrderBandUnset)
	want := strconv.FormatUint(uint64(topics.OrderBandUnset), 10)
	if got := pod.Annotations[captureOrderBandAnnotation]; got != want {
		t.Fatalf("order band annotation = %q, want unset sentinel %q", got, want)
	}
}

func TestCaptureJobSpecSetsOrderBandEnv(t *testing.T) {
	job := bugfixManager(true).captureJobSpec("s1", "c1", "node-1", "pod-uid-123", "containerd://abc", 5)
	env := envMap(job.Spec.Template.Spec.Containers[0].Env)
	if got := env["ORDER_BAND"]; got != "5" {
		t.Fatalf("ORDER_BAND env = %q, want %q", got, "5")
	}
}

func TestCaptureJobSpecDefaultsOrderBandToUnsetSentinel(t *testing.T) {
	job := bugfixManager(true).captureJobSpec("s1", "c1", "node-1", "pod-uid-123", "containerd://abc", topics.OrderBandUnset)
	env := envMap(job.Spec.Template.Spec.Containers[0].Env)
	want := strconv.FormatUint(uint64(topics.OrderBandUnset), 10)
	if got := env["ORDER_BAND"]; got != want {
		t.Fatalf("ORDER_BAND env = %q, want unset sentinel %q", got, want)
	}
}

func envMap(vars []corev1.EnvVar) map[string]string {
	out := make(map[string]string, len(vars))
	for _, v := range vars {
		out[v.Name] = v.Value
	}
	return out
}
