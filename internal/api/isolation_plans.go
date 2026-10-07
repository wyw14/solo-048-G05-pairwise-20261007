package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/isolation"
)

func (a *App) createIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Operator string `json:"operator"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Create(chi.URLParam(r, "sectionID"), input.Operator)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusCreated, plan)
}

func (a *App) listIsolationPlans(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.IsolationPlans.List())
}

func (a *App) getIsolationPlan(w http.ResponseWriter, r *http.Request) {
	plan, ok := a.IsolationPlans.Get(chi.URLParam(r, "planID"))
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("isolation plan %s not found", chi.URLParam(r, "planID")))
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (a *App) startIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Start(r.Context(), chi.URLParam(r, "planID"), input.Actor)
	a.writePlanResult(w, plan, err)
}

func (a *App) advanceIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Advance(r.Context(), chi.URLParam(r, "planID"), input.Actor)
	a.writePlanResult(w, plan, err)
}

func (a *App) resumeIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Resume(chi.URLParam(r, "planID"), input.Actor)
	a.writePlanResult(w, plan, err)
}

func (a *App) abortIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor  string `json:"actor"`
		Reason string `json:"reason"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Abort(chi.URLParam(r, "planID"), input.Actor, input.Reason)
	a.writePlanResult(w, plan, err)
}

// bumpPartitionRevision 模拟分区边界/拓扑修订变化（如分区重新划分发布），
// 用于冻结仍绑定旧修订的在途隔离计划。
func (a *App) bumpPartitionRevision(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	zone, err := a.Network.BumpPartitionRevision(chi.URLParam(r, "zoneID"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err := a.Audit.Accepted("partition.revision", zone.ID, input.Actor, "partition revision bumped", map[string]any{
		"partition_revision": zone.PartitionRevision,
	}); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, zone)
}

// writePlanResult 在计划被冻结/拒绝时同时返回最新计划快照与错误原因，
// 便于调用方读取 freeze 块决定 resume 或 abort。
func (a *App) writePlanResult(w http.ResponseWriter, plan isolation.Plan, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(responseEnvelope{Data: plan, Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(responseEnvelope{Data: plan})
}
