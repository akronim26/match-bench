// Package pipeline implements pipeline behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package pipeline

import (
	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/schemas/topics"
)

// AssembleOrder joins one sent event with its acks into a model.Order. Returns nil if
// the order was never delivered (no acks). EffectiveT3 is left zero; the streaming
// source's promotion pass sets it.
//
// hasSMPID reports whether a sent event carried a self-match-prevention id.
//
// Only SMPIDNone means absent. Id 0 is a VALID id and is honoured as one — treating it
// as absent would silently exempt 1 in every smp_id_count orders from the rule, which
// is a scoring hole rather than a safety measure.
//
// The trap this leaves: Go's zero value for the field is 0, so an OrderSentEvent built
// in code without setting SMPID reads as "participant 0". Every synthetic construction
// site must therefore set SMPIDNone explicitly (the validator's tests do). Events
// decoded from the wire always carry an explicit value, so the real path is unaffected.
func hasSMPID(id uint32) bool {
	return id != topics.SMPIDNone
}

func AssembleOrder(s topics.OrderSentEvent, acks []topics.OrderAckedEvent) *model.Order {
	if len(acks) == 0 {
		return nil
	}
	first := acks[0]
	return &model.Order{
		OrderID:     s.OrderID,
		Flow:        model.Flow{SrcIP: first.SrcIP, SrcPort: first.SrcPort},
		TCPSeq:      first.TCPSeq,
		T3Ns:        first.T3XDPIngressNS,
		Side:        model.SideFrom(s.Side),
		Price:       int64(s.Price) * int64(topics.TelemetryPriceScale),
		Qty:         s.Qty,
		Kind:        model.KindFrom(s.PayloadType, s.OrdType),
		OrigOrderID: preferOrig(s.OrigOrderID, acks),
		SMPID:       s.SMPID,
		HasSMPID:    hasSMPID(s.SMPID),
		Responses:   responses(acks),
	}
}

// preferOrig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func preferOrig(sentOrig string, acks []topics.OrderAckedEvent) string {
	if sentOrig != "" {
		return sentOrig
	}
	return firstOrig(acks)
}

// firstOrig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func firstOrig(acks []topics.OrderAckedEvent) string {
	for _, a := range acks {
		if a.OrigOrderID != "" {
			return a.OrigOrderID
		}
	}
	return ""
}

// responses performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func responses(acks []topics.OrderAckedEvent) []model.Response {
	out := make([]model.Response, 0, len(acks))
	for _, a := range acks {
		out = append(out, model.Response{
			ExecType:  a.ExecType,
			FillQty:   a.FillQty,
			FillPrice: a.FillPrice,
			T7Ns:      a.T7XDPEgressNS,
		})
	}
	return out
}
