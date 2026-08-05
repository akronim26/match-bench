// Package k8s defines tests for multi-port slots (QoL-7, §7.3 Shape A).
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package k8s

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/sandbox-orchestrator/internal/errors"
	corev1 "k8s.io/api/core/v1"
)

func TestPodSpecDeclaresOneContainerPortPerTarget(t *testing.T) {
	pod := bugfixManager(false).podSpec("s1", "c1", "img", []int{9898, 8080}, topics.OrderBandUnset)
	ports := pod.Spec.Containers[0].Ports
	if len(ports) != 2 {
		t.Fatalf("expected 2 container ports, got %d: %+v", len(ports), ports)
	}
	if ports[0].ContainerPort != 9898 || ports[1].ContainerPort != 8080 {
		t.Fatalf("unexpected port order: %+v", ports)
	}
}

func TestPodSpecReadinessProbeGatesPrimaryPort(t *testing.T) {
	pod := bugfixManager(false).podSpec("s1", "c1", "img", []int{9898, 8080}, topics.OrderBandUnset)
	probe := pod.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.TCPSocket == nil {
		t.Fatal("expected a TCP readiness probe")
	}
	if probe.TCPSocket.Port.IntValue() != 9898 {
		t.Fatalf("expected readiness probe on primary port 9898, got %d", probe.TCPSocket.Port.IntValue())
	}
}

func TestServiceSpecDeclaresOneServicePortPerTarget(t *testing.T) {
	svc := bugfixManager(false).serviceSpec("s1", []int{9898, 8080})
	if len(svc.Spec.Ports) != 2 {
		t.Fatalf("expected 2 service ports, got %d: %+v", len(svc.Spec.Ports), svc.Spec.Ports)
	}
	for _, p := range svc.Spec.Ports {
		if p.Name == "" {
			t.Errorf("service port %d missing required Name (needed once >1 port)", p.Port)
		}
	}
}

func TestCreateSlotRejectsEmptyPorts(t *testing.T) {
	if err := bugfixManager(false).CreateSlot(context.Background(), "s1", "c1", "img", nil, topics.OrderBandUnset); !errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("empty ports: got %v, want ErrInvalidRequest", err)
	}
}

func TestCreateSlotRejectsAnyUncapturablePortInMultiPortSet(t *testing.T) {
	m := bugfixManager(true)
	if err := m.CreateSlot(context.Background(), "s1", "c1", "img", []int{9898, 1234}, topics.OrderBandUnset); !errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("mixed capturable/uncapturable set: got %v, want ErrInvalidRequest", err)
	}
}

func TestContainerPortsExtractsDeclaredPorts(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Ports: []corev1.ContainerPort{{ContainerPort: 9898}, {ContainerPort: 8080}},
	}}}}
	got := containerPorts(pod)
	if len(got) != 2 || got[0] != 9898 || got[1] != 8080 {
		t.Fatalf("unexpected ports: %v", got)
	}
}

func TestContainerPortsHandlesNoContainers(t *testing.T) {
	if got := containerPorts(&corev1.Pod{}); got != nil {
		t.Fatalf("expected nil for a pod with no containers, got %v", got)
	}
}

func TestDialAllPortsFailsWithoutPodIP(t *testing.T) {
	ready, msg := dialAllPorts("", []int{9898})
	if ready {
		t.Fatal("expected not ready with empty pod IP")
	}
	if msg == "" {
		t.Fatal("expected a wait message")
	}
}

func TestDialAllPortsSucceedsWhenAllListen(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln1.Close()
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln2.Close()

	p1 := ln1.Addr().(*net.TCPAddr).Port
	p2 := ln2.Addr().(*net.TCPAddr).Port
	ready, msg := dialAllPorts("127.0.0.1", []int{p1, p2})
	if !ready {
		t.Fatalf("expected ready, got not-ready: %s", msg)
	}
}

func TestDialAllPortsFailsWhenOnePortIsClosed(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	open := ln.Addr().(*net.TCPAddr).Port

	// Grab an ephemeral port, then immediately close it so nothing listens.
	closedLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	closedPort := closedLn.Addr().(*net.TCPAddr).Port
	closedLn.Close()

	ready, msg := dialAllPorts("127.0.0.1", []int{open, closedPort})
	if ready {
		t.Fatal("expected not-ready when one declared port has no listener")
	}
	if msg == "" {
		t.Fatal("expected a wait message naming the pending port")
	}
}
