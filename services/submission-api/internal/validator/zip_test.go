// Package validator defines tests for zip behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validator

import (
	"testing"

	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
)

func TestValidatePortPolicyEnforcesPlatformPorts(t *testing.T) {
	cases := []struct {
		protocol string
		port     int
		wantErr  error
	}{
		{"FIX", 9898, nil},
		{"FIX", 8080, cerrs.ErrPortProtocolMismatch},
		{"REST", 8080, nil},
		{"REST", 9898, cerrs.ErrPortProtocolMismatch},
		{"WS", 8080, nil},
		{"WS", 9898, cerrs.ErrPortProtocolMismatch},
		{ProtocolAll, 1234, nil},
		{ProtocolAll, 9898, nil},
	}
	for _, c := range cases {
		got := validatePortPolicy(c.protocol, c.port)
		if got != c.wantErr {
			t.Errorf("validatePortPolicy(%q, %d) = %v, want %v", c.protocol, c.port, got, c.wantErr)
		}
	}
}

func TestValidProtocolsIncludesAllSentinel(t *testing.T) {
	// validProtocols is gone; declarations validate via topics.ParseProtocols.
	for _, p := range []string{"FIX", "REST", "WS", ProtocolAll, "REST,WS", "FIX,REST"} {
		if _, err := topics.ParseProtocols(p); err != nil {
			t.Fatalf("expected %q to be a valid protocol declaration: %v", p, err)
		}
	}
	for _, p := range []string{"HTTP", "FIX,ALL", "FIX,FIX", ""} {
		if _, err := topics.ParseProtocols(p); err == nil {
			t.Fatalf("expected %q to be rejected", p)
		}
	}
}

// TestPortIsOptionalAndDerived pins the 2026-08-02 decision: `port:` in
// benchmark.yaml is optional — ports are platform-mandated per protocol and
// the eBPF capture filter hardcodes them, so the field never carried
// information. Absent → derived from the PRIMARY protocol (first declared).
// Present-but-wrong on a single-protocol declaration is still rejected.
func TestPortIsOptionalAndDerived(t *testing.T) {
	cases := []struct {
		protocol string
		wantPort int
	}{
		{"FIX", 9898},
		{"REST", 8080},
		{"WS", 8080},
		{"REST,FIX", 8080}, // primary REST drives the derived port
		{"FIX,WS", 9898},
		{"ALL", 9898}, // ALL expands FIX-first
	}
	for _, c := range cases {
		cfg := BenchmarkConfig{Protocol: c.protocol} // Port absent (0)
		got, err := normalizePort(cfg)
		if err != nil {
			t.Errorf("%s: unexpected error %v", c.protocol, err)
			continue
		}
		if got != c.wantPort {
			t.Errorf("%s: derived port = %d, want %d", c.protocol, got, c.wantPort)
		}
	}
}
