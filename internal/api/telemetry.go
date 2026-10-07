package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/network"
	"heatpulse/internal/telemetry"
)

func (a *App) recordTelemetry(w http.ResponseWriter, r *http.Request) {
	var sample telemetry.Sample
	if err := decodeJSON(r, &sample); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.Telemetry.Add(sample); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if sample.Kind == telemetry.KindSupplyPressure || sample.Kind == telemetry.KindReturnPressure {
		a.syncZonePressure(sample.StationID)
	}
	writeJSON(w, http.StatusAccepted, sample)
}

// syncZonePressure folds the latest station pressure readings into the zone
// aggregate so that pressure-driven safeguards (e.g. isolation plan freezes)
// observe runtime changes.
func (a *App) syncZonePressure(stationID string) {
	station, ok := a.Stations.Get(stationID)
	if !ok {
		return
	}
	var supplySum, returnSum float64
	var supplyCount, returnCount int
	for _, item := range a.Stations.List() {
		if item.ZoneID != station.ZoneID {
			continue
		}
		if sample, ok := a.Telemetry.Latest(item.ID, telemetry.KindSupplyPressure); ok {
			supplySum += sample.Value
			supplyCount++
		}
		if sample, ok := a.Telemetry.Latest(item.ID, telemetry.KindReturnPressure); ok {
			returnSum += sample.Value
			returnCount++
		}
	}
	_, _ = a.Network.Update(station.ZoneID, func(zone *network.Zone) error {
		if supplyCount > 0 {
			zone.SupplyPressure = supplySum / float64(supplyCount)
		}
		if returnCount > 0 {
			zone.ReturnPressure = returnSum / float64(returnCount)
		}
		return nil
	})
}

func (a *App) evaluateHydraulic(w http.ResponseWriter, r *http.Request) {
	stationID := chi.URLParam(r, "stationID")
	snapshot, err := a.SnapshotsBuilder.Build(stationID)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	balance, err := a.Balancer.Evaluate(snapshot)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	incident, err := a.LeakProtection.Apply(balance)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	var protectionIncident any
	var isolationSession any
	if incident != nil && incident.Confirmed {
		raised, raiseErr := a.Protection.Raise(stationID, "hydraulic_leak", "critical", "confirmed mass imbalance")
		if raiseErr != nil {
			writeError(w, http.StatusConflict, raiseErr)
			return
		}
		protectionIncident = raised
		stationState, ok := a.Stations.Get(stationID)
		if ok {
			sections := a.Topology.SectionsForZone(stationState.ZoneID)
			if len(sections) > 0 {
				session, isolateErr := a.IsolationCoordinator.FromIncident(r.Context(), *incident, sections[0].ID)
				if isolateErr != nil {
					writeError(w, http.StatusConflict, isolateErr)
					return
				}
				isolationSession = session
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": snapshot, "balance": balance, "incident": incident,
		"protection_incident": protectionIncident, "isolation": isolationSession,
	})
}
