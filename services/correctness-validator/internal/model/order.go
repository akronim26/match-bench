// Package model implements order behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package model

// Flow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Flow struct {
	SrcIP   uint32
	SrcPort uint16
}

// Less applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (f Flow) Less(o Flow) bool {
	if f.SrcIP != o.SrcIP {
		return f.SrcIP < o.SrcIP
	}
	return f.SrcPort < o.SrcPort
}

type Side int

const (
	Buy Side = iota
	Sell
)

// SideFrom performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func SideFrom(s string) Side {
	if s == "SELL" {
		return Sell
	}
	return Buy
}

type Kind int

const (
	NewLimit Kind = iota
	NewMarket
	Cancel
	Replace
)

// KindFrom performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func KindFrom(payloadType, ordType string) Kind {
	switch payloadType {
	case "CANCEL":
		return Cancel
	case "REPLACE":
		return Replace
	default:
		if ordType == "MARKET" {
			return NewMarket
		}
		return NewLimit
	}
}

// String applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (k Kind) String() string {
	switch k {
	case NewLimit:
		return "NewLimit"
	case NewMarket:
		return "NewMarket"
	case Cancel:
		return "Cancel"
	case Replace:
		return "Replace"
	default:
		return "Unknown"
	}
}

// Response groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Response struct {
	ExecType  string
	FillQty   uint64
	FillPrice uint64 // fixed-point, scaled by TelemetryPriceScale
	T7Ns      uint64
}

// Order groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Order struct {
	OrderID     string
	Flow        Flow
	TCPSeq      uint32
	T3Ns        uint64 // request ingress (shared across the order's responses)
	EffectiveT3 uint64 // computed by replay ordering (HOL promotion)
	Side        Side
	Price       int64
	Qty         uint64
	Kind        Kind
	OrigOrderID string // referenced order for cancel/replace (from acked tag 41)
	// SMPID is the self-match-prevention id this order was sent under, taken from the
	// orders.sent telemetry event (the bot is the authority on what it assigned, so
	// the eBPF capture never has to parse FIX tag 7928).
	//
	// HasSMPID distinguishes "no id" from "id 0" — 0 is a VALID id, so the zero value
	// of this field cannot be used as the absent marker. An Order built without an
	// explicit id (any of the book's test helpers, and any future construction site
	// that predates SMP) is therefore UNCONSTRAINED by default, which is both the safe
	// direction and what pass-2 traffic actually is. Conflating the two would make
	// every id-less order look like participant 0 and, under skip-and-continue, stop
	// an engine matching anything at all.
	SMPID     uint32
	HasSMPID  bool
	Responses []Response
}
