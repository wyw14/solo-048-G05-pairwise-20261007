package isolation

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/network"
	"heatpulse/internal/protection"
	"heatpulse/internal/valve"
)

// pressureDriftTolerance is the maximum zone pressure deviation (MPa) from the
// plan baseline tolerated before subsequent plan actions are frozen.
const pressureDriftTolerance = 0.05

type StepKind string

const (
	StepCloseBoundary StepKind = "close_boundary"
	StepOpenRelief    StepKind = "open_relief"
	StepVerify        StepKind = "verify"
	StepRecover       StepKind = "recover"
)

type StepStatus string

const (
	StepPending StepStatus = "pending"
	StepDone    StepStatus = "done"
	StepSkipped StepStatus = "skipped"
)

type Step struct {
	Sequence int        `json:"sequence"`
	Kind     StepKind   `json:"kind"`
	ValveID  string     `json:"valve_id,omitempty"`
	Status   StepStatus `json:"status"`
	Detail   string     `json:"detail,omitempty"`
	At       time.Time  `json:"at,omitempty"`
}

type PlanStatus string

const (
	PlanDraft     PlanStatus = "draft"
	PlanExecuting PlanStatus = "executing"
	PlanFrozen    PlanStatus = "frozen"
	PlanCompleted PlanStatus = "completed"
	PlanAborted   PlanStatus = "aborted"
)

// Plan is an operator-bound, revision-pinned isolation procedure for one
// section: close boundary valves, open the relief valve, verify isolation,
// then hand the zone back to recovery.
type Plan struct {
	ID                string     `json:"id"`
	SectionID         string     `json:"section_id"`
	ZoneID            string     `json:"zone_id"`
	Revision          uint64     `json:"revision"`
	Actor             string     `json:"actor"`
	Status            PlanStatus `json:"status"`
	Steps             []Step     `json:"steps"`
	BaselineSupply    float64    `json:"baseline_supply_pressure"`
	BaselineReturn    float64    `json:"baseline_return_pressure"`
	BaselineIncidents []string   `json:"baseline_incidents"`
	FreezeReason      string     `json:"freeze_reason,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	CompletedAt       time.Time  `json:"completed_at,omitempty"`
}

func (p Plan) clone() Plan {
	copy := p
	copy.Steps = append([]Step(nil), p.Steps...)
	copy.BaselineIncidents = append([]string(nil), p.BaselineIncidents...)
	return copy
}

func (p Plan) nextPending() int {
	for i, step := range p.Steps {
		if step.Status == StepPending {
			return i
		}
	}
	return -1
}

type PlanService struct {
	topology   *network.Topology
	state      *network.State
	registry   *network.Registry
	relief     *valve.ReliefService
	boundaries *valve.BoundaryService
	protection *protection.Service
	audit      *audit.Recorder
	mu         sync.RWMutex
	plans      map[string]Plan
}

func NewPlanService(topology *network.Topology, state *network.State, registry *network.Registry, relief *valve.ReliefService, boundaries *valve.BoundaryService, protectionService *protection.Service, recorder *audit.Recorder) *PlanService {
	return &PlanService{
		topology: topology, state: state, registry: registry, relief: relief,
		boundaries: boundaries, protection: protectionService, audit: recorder,
		plans: make(map[string]Plan),
	}
}

// Create builds an ordered isolation plan from the network topology: boundary
// valves of the section in deterministic order, then its relief valve, then
// verification and recovery. The plan pins the zone recovery revision, the
// responsible operator and the current pressure/protection baseline.
func (s *PlanService) Create(sectionID, actor string) (Plan, error) {
	if actor == "" {
		return Plan{}, fmt.Errorf("operator is required")
	}
	section, err := s.topology.Section(sectionID)
	if err != nil {
		return Plan{}, err
	}
	zone, ok := s.state.Get(section.ZoneID)
	if !ok {
		return Plan{}, fmt.Errorf("zone %s not found", section.ZoneID)
	}
	valves := append([]string(nil), section.BoundaryValves...)
	sort.Strings(valves)
	steps := make([]Step, 0, len(valves)+3)
	for _, valveID := range valves {
		steps = append(steps, Step{Sequence: len(steps) + 1, Kind: StepCloseBoundary, ValveID: valveID, Status: StepPending})
	}
	steps = append(steps, Step{Sequence: len(steps) + 1, Kind: StepOpenRelief, ValveID: section.ReliefValve, Status: StepPending})
	steps = append(steps, Step{Sequence: len(steps) + 1, Kind: StepVerify, Status: StepPending})
	steps = append(steps, Step{Sequence: len(steps) + 1, Kind: StepRecover, Status: StepPending})
	now := time.Now().UTC()
	plan := Plan{
		ID: uuid.NewString(), SectionID: sectionID, ZoneID: section.ZoneID,
		Revision: zone.RecoveryRevision, Actor: actor, Status: PlanDraft, Steps: steps,
		BaselineSupply: zone.SupplyPressure, BaselineReturn: zone.ReturnPressure,
		BaselineIncidents: s.activeIncidents(section.ZoneID),
		CreatedAt:         now, UpdatedAt: now,
	}
	s.mu.Lock()
	s.plans[plan.ID] = plan
	s.mu.Unlock()
	if err := s.audit.Accepted("isolation_plan.created", plan.ID, actor, "isolation plan created", map[string]any{
		"section_id": sectionID, "zone_id": section.ZoneID, "revision": plan.Revision, "steps": len(steps),
	}); err != nil {
		return Plan{}, err
	}
	return plan.clone(), nil
}

// Advance executes the next pending step. Before any action it re-checks the
// bound revision, zone pressure drift and protection state; any change freezes
// the plan and blocks all subsequent actions until Resume re-baselines it.
func (s *PlanService) Advance(ctx context.Context, planID, actor string) (Plan, error) {
	s.mu.Lock()
	plan, ok := s.plans[planID]
	if !ok {
		s.mu.Unlock()
		return Plan{}, fmt.Errorf("isolation plan %s not found", planID)
	}
	if err := checkActor(plan, actor); err != nil {
		s.mu.Unlock()
		return plan.clone(), err
	}
	if plan.Status == PlanFrozen {
		s.mu.Unlock()
		return plan.clone(), fmt.Errorf("isolation plan %s is frozen: %s", planID, plan.FreezeReason)
	}
	if plan.Status != PlanDraft && plan.Status != PlanExecuting {
		s.mu.Unlock()
		return plan.clone(), fmt.Errorf("isolation plan %s is %s", planID, plan.Status)
	}
	if reason := s.freezeReason(plan); reason != "" {
		plan.Status = PlanFrozen
		plan.FreezeReason = reason
		plan.UpdatedAt = time.Now().UTC()
		s.plans[plan.ID] = plan
		s.mu.Unlock()
		_ = s.audit.Rejected("isolation_plan.frozen", plan.ID, actor, reason, map[string]any{"section_id": plan.SectionID})
		return plan.clone(), fmt.Errorf("isolation plan %s frozen: %s", planID, reason)
	}
	index := plan.nextPending()
	if index < 0 {
		s.mu.Unlock()
		return plan.clone(), fmt.Errorf("isolation plan %s has no pending steps", planID)
	}
	plan.Status = PlanExecuting
	step, err := s.executeStep(ctx, plan, plan.Steps[index])
	if err != nil {
		plan.UpdatedAt = time.Now().UTC()
		s.plans[plan.ID] = plan
		s.mu.Unlock()
		_ = s.audit.Rejected("isolation_plan.step_failed", plan.ID, actor, err.Error(), map[string]any{"sequence": plan.Steps[index].Sequence})
		return plan.clone(), err
	}
	plan.Steps[index] = step
	plan.UpdatedAt = time.Now().UTC()
	if plan.nextPending() < 0 {
		plan.Status = PlanCompleted
		plan.CompletedAt = plan.UpdatedAt
	}
	s.plans[plan.ID] = plan
	s.mu.Unlock()
	if plan.Status == PlanCompleted {
		if err := s.audit.Accepted("isolation_plan.completed", plan.ID, actor, "isolation plan completed", map[string]any{"section_id": plan.SectionID}); err != nil {
			return plan.clone(), err
		}
	}
	return plan.clone(), nil
}

// Resume lifts a frozen plan by re-binding it to the current zone revision and
// re-baselining pressure and protection state. Only the bound operator may resume.
func (s *PlanService) Resume(planID, actor string) (Plan, error) {
	s.mu.Lock()
	plan, ok := s.plans[planID]
	if !ok {
		s.mu.Unlock()
		return Plan{}, fmt.Errorf("isolation plan %s not found", planID)
	}
	if err := checkActor(plan, actor); err != nil {
		s.mu.Unlock()
		return plan.clone(), err
	}
	if plan.Status != PlanFrozen {
		s.mu.Unlock()
		return plan.clone(), fmt.Errorf("isolation plan %s is %s, not frozen", planID, plan.Status)
	}
	zone, ok := s.state.Get(plan.ZoneID)
	if !ok {
		s.mu.Unlock()
		return plan.clone(), fmt.Errorf("zone %s not found", plan.ZoneID)
	}
	plan.Status = PlanExecuting
	plan.FreezeReason = ""
	plan.Revision = zone.RecoveryRevision
	plan.BaselineSupply = zone.SupplyPressure
	plan.BaselineReturn = zone.ReturnPressure
	plan.BaselineIncidents = s.activeIncidents(plan.ZoneID)
	plan.UpdatedAt = time.Now().UTC()
	s.plans[plan.ID] = plan
	s.mu.Unlock()
	if err := s.audit.Accepted("isolation_plan.resumed", plan.ID, actor, "isolation plan resumed with new baseline", map[string]any{"revision": plan.Revision}); err != nil {
		return plan.clone(), err
	}
	return plan.clone(), nil
}

// Abort cancels a plan that has not completed; pending steps are skipped.
func (s *PlanService) Abort(planID, actor string) (Plan, error) {
	s.mu.Lock()
	plan, ok := s.plans[planID]
	if !ok {
		s.mu.Unlock()
		return Plan{}, fmt.Errorf("isolation plan %s not found", planID)
	}
	if err := checkActor(plan, actor); err != nil {
		s.mu.Unlock()
		return plan.clone(), err
	}
	if plan.Status == PlanCompleted || plan.Status == PlanAborted {
		s.mu.Unlock()
		return plan.clone(), fmt.Errorf("isolation plan %s is %s", planID, plan.Status)
	}
	for i := range plan.Steps {
		if plan.Steps[i].Status == StepPending {
			plan.Steps[i].Status = StepSkipped
		}
	}
	plan.Status = PlanAborted
	plan.UpdatedAt = time.Now().UTC()
	plan.CompletedAt = plan.UpdatedAt
	s.plans[plan.ID] = plan
	s.mu.Unlock()
	if err := s.audit.Accepted("isolation_plan.aborted", plan.ID, actor, "isolation plan aborted", map[string]any{"section_id": plan.SectionID}); err != nil {
		return plan.clone(), err
	}
	return plan.clone(), nil
}

func (s *PlanService) Get(planID string) (Plan, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	plan, ok := s.plans[planID]
	return plan.clone(), ok
}

func (s *PlanService) List() []Plan {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Plan, 0, len(s.plans))
	for _, plan := range s.plans {
		result = append(result, plan.clone())
	}
	return result
}

func checkActor(plan Plan, actor string) error {
	if actor == "" {
		return fmt.Errorf("operator is required")
	}
	if actor != plan.Actor {
		return fmt.Errorf("isolation plan %s is bound to operator %s", plan.ID, plan.Actor)
	}
	return nil
}

// freezeReason reports why the plan must freeze, or "" when the bound
// revision, zone pressures and protection state all match the baseline.
func (s *PlanService) freezeReason(plan Plan) string {
	zone, ok := s.state.Get(plan.ZoneID)
	if !ok {
		return fmt.Sprintf("zone %s not found", plan.ZoneID)
	}
	if zone.RecoveryRevision != plan.Revision {
		return fmt.Sprintf("zone recovery revision changed from %d to %d", plan.Revision, zone.RecoveryRevision)
	}
	if math.Abs(zone.SupplyPressure-plan.BaselineSupply) > pressureDriftTolerance ||
		math.Abs(zone.ReturnPressure-plan.BaselineReturn) > pressureDriftTolerance {
		return fmt.Sprintf("zone pressure drifted beyond %.2f MPa from baseline", pressureDriftTolerance)
	}
	current := s.activeIncidents(plan.ZoneID)
	if !sameStrings(current, plan.BaselineIncidents) {
		return "active protection incidents changed"
	}
	return ""
}

// activeIncidents lists active protection incident IDs for stations bound to
// the zone, in deterministic order.
func (s *PlanService) activeIncidents(zoneID string) []string {
	stations := make(map[string]bool)
	for _, stationID := range s.registry.RequiredForZone(zoneID) {
		stations[stationID] = true
	}
	ids := make([]string, 0)
	for _, incident := range s.protection.List() {
		if incident.Active && stations[incident.StationID] {
			ids = append(ids, incident.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *PlanService) executeStep(ctx context.Context, plan Plan, step Step) (Step, error) {
	switch step.Kind {
	case StepCloseBoundary:
		if _, err := s.boundaries.CloseIsolating(ctx, step.ValveID, plan.ID); err != nil {
			return step, err
		}
		if _, err := s.state.Update(plan.ZoneID, func(zone *network.Zone) error {
			if zone.Phase == network.PhaseActive || zone.Phase == network.PhaseWarming {
				zone.Phase = network.PhaseIsolating
			}
			return nil
		}); err != nil {
			return step, err
		}
		step.Detail = "boundary valve closed"
	case StepOpenRelief:
		if _, err := s.relief.Open(ctx, step.ValveID, plan.ID); err != nil {
			return step, err
		}
		step.Detail = "relief valve opened"
	case StepVerify:
		if err := s.verify(plan); err != nil {
			return step, err
		}
		if _, err := s.state.Update(plan.ZoneID, func(zone *network.Zone) error {
			zone.Phase = network.PhaseIsolated
			return nil
		}); err != nil {
			return step, err
		}
		step.Detail = "isolation verified"
	case StepRecover:
		if _, err := s.state.Update(plan.ZoneID, func(zone *network.Zone) error {
			if zone.Phase != network.PhaseIsolated {
				return fmt.Errorf("zone %s must be isolated before recovery, currently %s", plan.ZoneID, zone.Phase)
			}
			zone.Phase = network.PhaseRecovering
			return nil
		}); err != nil {
			return step, err
		}
		step.Detail = "zone handed to recovery"
	default:
		return step, fmt.Errorf("unknown step kind %s", step.Kind)
	}
	step.Status = StepDone
	step.At = time.Now().UTC()
	return step, nil
}

func (s *PlanService) verify(plan Plan) error {
	for _, step := range plan.Steps {
		if step.Kind == StepCloseBoundary && !s.boundaries.IsClosed(step.ValveID) {
			return fmt.Errorf("boundary valve %s is not closed", step.ValveID)
		}
		if step.Kind == StepOpenRelief && !s.relief.IsOpen(step.ValveID) {
			return fmt.Errorf("relief valve %s is not open", step.ValveID)
		}
	}
	return nil
}
