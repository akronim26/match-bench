// Package handler implements slot behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/sandbox-orchestrator/internal/errors"
	"github.com/iicpc/sandbox-orchestrator/internal/k8s"
	"github.com/iicpc/sandbox-orchestrator/internal/store"
)

// createSlotRequest groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type createSlotRequest struct {
	SlotID       string `json:"slot_id"`
	ContestantID string `json:"contestant_id"` // stamped onto the eBPF capture's latency events
	Image        string `json:"image"`
	Port         int    `json:"port,omitempty"`  // back-compat single-port slot; ignored if Ports is set
	Ports        []int  `json:"ports,omitempty"` // one ContainerPort/ServicePort per entry

	// OrderBand is the session's leased exclusive orders.acked partition
	// band, threaded onto the capture container as ORDER_BAND. A pointer so
	// the JSON zero value (0) can be told apart from "field omitted" — 0 is
	// a legitimate band index, so a plain uint32 would silently misread an
	// absent field as band 0. Callers that don't set it (or send explicit
	// null) get topics.OrderBandUnset, the same sentinel bot-fleet-controller
	// and schemas/{go,rust} use for "fall back to hash-derived banding".
	OrderBand *uint32 `json:"order_band"`
}

// orderBand resolves the request's order band, defaulting to
// topics.OrderBandUnset when the field was omitted or explicitly null.
func (r createSlotRequest) orderBand() uint32 {
	if r.OrderBand == nil {
		return topics.OrderBandUnset
	}
	return *r.OrderBand
}

// resolvePorts normalizes a createSlotRequest onto its declared port list:
// Ports if set, otherwise a single-entry list built from Port so old
// single-port callers keep working unchanged.
func (r createSlotRequest) resolvePorts() []int {
	if len(r.Ports) > 0 {
		return r.Ports
	}
	if r.Port > 0 {
		return []int{r.Port}
	}
	return nil
}

// slotResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type slotResponse struct {
	SlotID   string       `json:"slot_id"`
	State    string       `json:"state"`
	Message  string       `json:"message"`
	Endpoint endpointJSON `json:"endpoint"`
}

// endpointJSON groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type endpointJSON struct {
	Host  string `json:"host"`
	Port  int    `json:"port"`
	Ports []int  `json:"ports,omitempty"`
}

// CreateSlot performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func CreateSlot(mgr *k8s.Manager, slots *store.SlotStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createSlotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		ports := req.resolvePorts()
		if req.SlotID == "" || req.Image == "" || len(ports) == 0 {
			writeError(w, http.StatusBadRequest, "slot_id, image, and port (or ports) are required")
			return
		}
		for _, p := range ports {
			if p <= 0 {
				writeError(w, http.StatusBadRequest, "ports must be positive")
				return
			}
		}

		ctx := r.Context()
		existing, exists := slots.Get(req.SlotID)
		if exists && existing.Image == req.Image {
			state, msg, err := mgr.Refresh(ctx, req.SlotID)
			if err != nil && !errors.Is(err, cerrs.ErrSlotNotFound) {
				recordSlot("refresh", "error", "")
				log.ErrorContext(ctx, "refresh slot", "slot_id", req.SlotID, "error", err)
				writeError(w, http.StatusInternalServerError, "refresh slot: "+err.Error())
				return
			}
			if !errors.Is(err, cerrs.ErrSlotNotFound) {
				existing.State = state
				existing.Message = msg
				slots.Put(existing)
			}
			recordSlot("create_idempotent", "ok", string(existing.State))
			writeJSON(w, http.StatusOK, toResponse(existing))
			return
		}

		start := time.Now()
		if err := mgr.CreateSlot(ctx, req.SlotID, req.ContestantID, req.Image, ports, req.orderBand()); err != nil {
			metrics.Histogram("slot_create_duration_seconds", "Sandbox slot create duration in seconds.", metrics.Labels("result", "error"), metrics.SinceSeconds(start))
			switch {
			case errors.Is(err, cerrs.ErrSlotImageMismatch):
				recordSlot("create", "conflict", "")
				writeError(w, http.StatusConflict, err.Error())
			case errors.Is(err, cerrs.ErrInvalidRequest):
				writeError(w, http.StatusBadRequest, err.Error())
			default:
				recordSlot("create", "error", "")
				log.ErrorContext(ctx, "create slot", "slot_id", req.SlotID, "error", err)
				writeError(w, http.StatusInternalServerError, "create slot: "+err.Error())
			}
			return
		}
		metrics.Histogram("slot_create_duration_seconds", "Sandbox slot create duration in seconds.", metrics.Labels("result", "ok"), metrics.SinceSeconds(start))

		state, msg, _ := mgr.Refresh(ctx, req.SlotID)
		slot := &store.Slot{
			SlotID:    req.SlotID,
			Image:     req.Image,
			Port:      ports[0],
			Ports:     ports,
			State:     state,
			Message:   msg,
			Endpoint:  store.Endpoint{Host: k8s.ServiceFQDN(req.SlotID, mgr.Namespace()), Port: ports[0]},
			CreatedAt: time.Now().UTC(),
		}
		slots.Put(slot)
		recordSlot("create", "ok", string(state))
		log.InfoContext(ctx, "slot created", "slot_id", req.SlotID, "image", req.Image, "ports", ports)
		writeJSON(w, http.StatusCreated, toResponse(slot))
	}
}

// GetSlot performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func GetSlot(mgr *k8s.Manager, slots *store.SlotStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slotID := chi.URLParam(r, "slot_id")
		if slotID == "" {
			writeError(w, http.StatusBadRequest, "missing slot_id")
			return
		}

		ctx := r.Context()
		slot, ok := slots.Get(slotID)
		if !ok {
			writeError(w, http.StatusNotFound, "slot not found")
			return
		}

		state, msg, err := mgr.Refresh(ctx, slotID)
		if errors.Is(err, cerrs.ErrSlotNotFound) {
			slots.Delete(slotID)
			recordSlot("refresh", "not_found", "")
			writeError(w, http.StatusNotFound, "slot not found")
			return
		}
		if err != nil {
			recordSlot("refresh", "error", "")
			log.ErrorContext(ctx, "refresh slot", "slot_id", slotID, "error", err)
			writeError(w, http.StatusInternalServerError, "refresh slot: "+err.Error())
			return
		}

		slot.State = state
		slot.Message = msg
		slots.Put(slot)
		recordSlot("refresh", "ok", string(state))
		writeJSON(w, http.StatusOK, toResponse(slot))
	}
}

// DeleteSlot performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func DeleteSlot(mgr *k8s.Manager, slots *store.SlotStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slotID := chi.URLParam(r, "slot_id")
		if slotID == "" {
			writeError(w, http.StatusBadRequest, "missing slot_id")
			return
		}

		ctx := r.Context()
		if slot, ok := slots.Get(slotID); ok {
			slot.State = store.StateTerminating
			slot.Message = "delete requested"
			slots.Put(slot)
		}
		if err := mgr.DeleteSlot(ctx, slotID); err != nil {
			recordSlot("delete", "error", "")
			log.ErrorContext(ctx, "delete slot", "slot_id", slotID, "error", err)
			writeError(w, http.StatusInternalServerError, "delete slot: "+err.Error())
			return
		}
		slots.Delete(slotID)
		recordSlot("delete", "ok", "")
		log.InfoContext(ctx, "slot released", "slot_id", slotID)
		w.WriteHeader(http.StatusNoContent)
	}
}

// recordSlot performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordSlot(operation, result, state string) {
	metrics.Counter("slots_operations_total", "Sandbox slot operations by operation and result.", metrics.Labels("operation", operation, "result", result), 1)
	if state != "" {
		metrics.Counter("slot_refresh_total", "Sandbox slot refreshes by state and result.", metrics.Labels("state", state, "result", result), 1)
	}
}

// toResponse performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func toResponse(slot *store.Slot) slotResponse {
	return slotResponse{
		SlotID:  slot.SlotID,
		State:   string(slot.State),
		Message: slot.Message,
		Endpoint: endpointJSON{
			Host:  slot.Endpoint.Host,
			Port:  slot.Endpoint.Port,
			Ports: slot.Ports,
		},
	}
}
