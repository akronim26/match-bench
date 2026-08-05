// Package handler defines tests for slot request port resolution.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import "testing"

func TestResolvePortsPrefersPortsOverPort(t *testing.T) {
	req := createSlotRequest{Port: 8080, Ports: []int{9898, 8080}}
	got := req.resolvePorts()
	if len(got) != 2 || got[0] != 9898 || got[1] != 8080 {
		t.Fatalf("expected Ports to win, got %v", got)
	}
}

func TestResolvePortsFallsBackToLegacyPort(t *testing.T) {
	req := createSlotRequest{Port: 9898}
	got := req.resolvePorts()
	if len(got) != 1 || got[0] != 9898 {
		t.Fatalf("expected legacy single-port fallback, got %v", got)
	}
}

func TestResolvePortsEmptyWhenNeitherSet(t *testing.T) {
	req := createSlotRequest{}
	if got := req.resolvePorts(); got != nil {
		t.Fatalf("expected nil ports, got %v", got)
	}
}
