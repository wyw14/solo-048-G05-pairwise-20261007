package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (a *App) createIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Create(chi.URLParam(r, "sectionID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, plan)
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

func (a *App) advanceIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Advance(r.Context(), chi.URLParam(r, "planID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
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
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (a *App) abortIsolationPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.IsolationPlans.Abort(chi.URLParam(r, "planID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}
