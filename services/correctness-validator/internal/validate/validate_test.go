// Package validate defines the shared fixture helpers for this package's tests.
//
// The scoring tests that used to live here drove the batch validate.Run; they now drive
// StreamValidator and live in stream_test.go (per-class scenarios), stream_smp_test.go
// (self-match, missed fills, determinism) and v2_test.go (ordering/priority).
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validate

import (
	"github.com/iicpc/correctness-validator/internal/model"
)

// fill is an execution report claiming `qty` filled at `price`.
func fill(qty, price uint64) model.Response {
	return model.Response{ExecType: "2", FillQty: qty, FillPrice: price}
}

// order builds an id-less (unconstrained) order: HasSMPID stays false, so it matches
// anything. Use orderSMP when the test needs self-match-prevention identity.
func order(id string, kind model.Kind, side model.Side, price int64, qty uint64, resp ...model.Response) *model.Order {
	return &model.Order{OrderID: id, Kind: kind, Side: side, Price: price, Qty: qty, Responses: resp}
}

// orderSMP is `order` with a self-match-prevention id attached. Self-trade is keyed on
// this, not on the task id embedded in the order id, so a test that wants a self-trade
// must give both sides the same SMP id.
func orderSMP(id string, kind model.Kind, side model.Side, price int64, qty uint64, smp uint32, resp ...model.Response) *model.Order {
	o := order(id, kind, side, price, qty, resp...)
	o.SMPID, o.HasSMPID = smp, true
	return o
}

// violationsOfType counts retained violation examples of one class.
func violationsOfType(r Report, vt ViolationType) int {
	n := 0
	for _, v := range r.Violations {
		if v.Type == vt {
			n++
		}
	}
	return n
}
