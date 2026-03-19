package simulator

import (
	"context"
	"encoding/json"
	"net/http"
)

type HTTPAPI struct {
	manager *ScaleManager
	ctx     context.Context
}

func NewHTTPAPI(ctx context.Context, manager *ScaleManager) *HTTPAPI {
	return &HTTPAPI{manager: manager, ctx: ctx}
}

func (a *HTTPAPI) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.HandleFunc("/api/status", a.handleStatus)
	mux.HandleFunc("/api/scale", a.handleScale)
	return mux
}

func (a *HTTPAPI) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (a *HTTPAPI) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.manager.Status())
}

func (a *HTTPAPI) handleScale(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if err := a.ctx.Err(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "simulator is shutting down"})
		return
	}
	var req struct {
		TargetPumps int `json:"target_pumps"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	prev := a.manager.CurrentCount()
	if err := a.manager.ScaleTo(r.Context(), req.TargetPumps); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"previous_pumps": prev,
		"target_pumps":   req.TargetPumps,
		"current_pumps":  a.manager.CurrentCount(),
	})
}
