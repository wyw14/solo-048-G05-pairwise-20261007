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

// PlanStatus 是隔离计划的生命周期状态。
type PlanStatus string

const (
	PlanDraft     PlanStatus = "draft"
	PlanRunning   PlanStatus = "running"
	PlanFrozen    PlanStatus = "frozen"
	PlanCompleted PlanStatus = "completed"
	PlanAborted   PlanStatus = "aborted"
)

// StepKind 标识计划中的一个有序动作。
type StepKind string

const (
	StepReliefOpen    StepKind = "relief_open"
	StepBoundaryClose StepKind = "boundary_close"
	StepVerify        StepKind = "verify_isolation"
	StepBoundaryOpen  StepKind = "boundary_open"
	StepReliefClose   StepKind = "relief_close"
)

type StepStatus string

const (
	StepPending StepStatus = "pending"
	StepDone    StepStatus = "done"
)

// Freeze 触发原因。Resumable 为 false 时（分区修订漂移）计划只能作废重编。
const (
	FreezePartitionRevision = "partition_revision_changed"
	FreezePressureDrift     = "pressure_changed"
	FreezeProtection        = "protection_state_changed"
	FreezeVerification      = "verification_failed"
)

// ProtectionWatcher 提供保护状态快照；*protection.Service 天然满足。
type ProtectionWatcher interface {
	List() []protection.Incident
}

type Baseline struct {
	PartitionRevision uint64   `json:"partition_revision"`
	SupplyPressure    float64  `json:"supply_pressure"`
	ReturnPressure    float64  `json:"return_pressure"`
	Incidents         []string `json:"incidents"`
}

type FreezeReason struct {
	Kind      string    `json:"kind"`
	Message   string    `json:"message"`
	Resumable bool      `json:"resumable"`
	At        time.Time `json:"at"`
}

type VerificationProof struct {
	ReliefOpen       bool            `json:"relief_open"`
	BoundariesClosed map[string]bool `json:"boundaries_closed"`
	At               time.Time       `json:"at"`
}

type PlanStep struct {
	Sequence int                  `json:"sequence"`
	Kind     StepKind             `json:"kind"`
	ValveID  string               `json:"valve_id,omitempty"`
	Label    string               `json:"label"`
	Status   StepStatus           `json:"status"`
	Relief   *valve.ReliefProof   `json:"relief_proof,omitempty"`
	Boundary *valve.BoundaryProof `json:"boundary_proof,omitempty"`
	Verify   *VerificationProof   `json:"verification,omitempty"`
	DoneAt   time.Time            `json:"done_at,omitempty"`
}

// Plan 是绑定到具体分区修订与操作人的有序隔离/恢复计划。
type Plan struct {
	ID             string        `json:"id"`
	SectionID      string        `json:"section_id"`
	ZoneID         string        `json:"zone_id"`
	Status         PlanStatus    `json:"status"`
	Operator       string        `json:"operator"`
	ReliefValve    string        `json:"relief_valve"`
	BoundaryValves []string      `json:"boundary_valves"`
	Baseline       Baseline      `json:"baseline"`
	Steps          []PlanStep    `json:"steps"`
	Freeze         *FreezeReason `json:"freeze,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	StartedAt      time.Time     `json:"started_at,omitempty"`
	CompletedAt    time.Time     `json:"completed_at,omitempty"`
}

func (p Plan) Clone() Plan {
	copy := p
	copy.BoundaryValves = append([]string(nil), p.BoundaryValves...)
	copy.Baseline.Incidents = append([]string(nil), p.Baseline.Incidents...)
	copy.Steps = make([]PlanStep, len(p.Steps))
	for i, step := range p.Steps {
		copy.Steps[i] = step
		if step.Relief != nil {
			proof := *step.Relief
			copy.Steps[i].Relief = &proof
		}
		if step.Boundary != nil {
			proof := *step.Boundary
			copy.Steps[i].Boundary = &proof
		}
		if step.Verify != nil {
			verify := *step.Verify
			copy.Steps[i].Verify = &verify
		}
	}
	if p.Freeze != nil {
		freeze := *p.Freeze
		copy.Freeze = &freeze
	}
	return copy
}

type PlanService struct {
	topology   *network.Topology
	state      *network.State
	registry   *network.Registry
	relief     *valve.ReliefService
	boundaries *valve.BoundaryService
	protection ProtectionWatcher
	audit      *audit.Recorder
	tolerance  float64
	mu         sync.Mutex
	plans      map[string]Plan
	activeSect map[string]string
}

func NewPlanService(topology *network.Topology, state *network.State, registry *network.Registry,
	relief *valve.ReliefService, boundaries *valve.BoundaryService, protection ProtectionWatcher,
	recorder *audit.Recorder, pressureTolerance float64) *PlanService {
	if pressureTolerance <= 0 {
		pressureTolerance = 0.05
	}
	return &PlanService{
		topology: topology, state: state, registry: registry, relief: relief, boundaries: boundaries,
		protection: protection, audit: recorder, tolerance: pressureTolerance,
		plans: make(map[string]Plan), activeSect: make(map[string]string),
	}
}

// Create 根据拓扑选取泄压阀与边界阀，生成 泄压→关阀→验证→恢复（反向开阀→关泄压） 的有序计划，
// 并把计划绑定到当前分区修订与操作人。
func (s *PlanService) Create(sectionID, operator string) (Plan, error) {
	if operator == "" {
		return Plan{}, fmt.Errorf("operator is required to bind an isolation plan")
	}
	section, err := s.topology.Section(sectionID)
	if err != nil {
		return Plan{}, err
	}
	zone, ok := s.state.Get(section.ZoneID)
	if !ok {
		return Plan{}, fmt.Errorf("zone %s not found", section.ZoneID)
	}

	baseline := Baseline{
		PartitionRevision: zone.PartitionRevision,
		SupplyPressure:    zone.SupplyPressure,
		ReturnPressure:    zone.ReturnPressure,
		Incidents:         s.activeIncidents(section.ZoneID),
	}
	valves := append([]string(nil), section.BoundaryValves...)
	steps := make([]PlanStep, 0, len(valves)*2+2)
	sequence := 1
	steps = append(steps, PlanStep{
		Sequence: sequence, Kind: StepReliefOpen, ValveID: section.ReliefValve, Status: StepPending,
		Label: fmt.Sprintf("open relief valve %s before closing any boundary", section.ReliefValve),
	})
	for _, valveID := range valves {
		sequence++
		steps = append(steps, PlanStep{
			Sequence: sequence, Kind: StepBoundaryClose, ValveID: valveID, Status: StepPending,
			Label: fmt.Sprintf("close boundary valve %s", valveID),
		})
	}
	sequence++
	steps = append(steps, PlanStep{
		Sequence: sequence, Kind: StepVerify, Status: StepPending,
		Label: "verify relief path open and every boundary valve closed",
	})
	for i := len(valves) - 1; i >= 0; i-- {
		sequence++
		steps = append(steps, PlanStep{
			Sequence: sequence, Kind: StepBoundaryOpen, ValveID: valves[i], Status: StepPending,
			Label: fmt.Sprintf("reopen boundary valve %s with relief path held", valves[i]),
		})
	}
	sequence++
	steps = append(steps, PlanStep{
		Sequence: sequence, Kind: StepReliefClose, ValveID: section.ReliefValve, Status: StepPending,
		Label: fmt.Sprintf("close relief valve %s after repressurization", section.ReliefValve),
	})

	plan := Plan{
		ID: uuid.NewString(), SectionID: sectionID, ZoneID: section.ZoneID, Status: PlanDraft,
		Operator: operator, ReliefValve: section.ReliefValve, BoundaryValves: valves,
		Baseline: baseline, Steps: steps, CreatedAt: time.Now().UTC(),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if planID := s.activeSect[sectionID]; planID != "" {
		return Plan{}, fmt.Errorf("section %s already has active isolation plan %s", sectionID, planID)
	}
	s.plans[plan.ID] = plan
	s.activeSect[sectionID] = plan.ID
	_ = s.audit.Accepted("isolation_plan.create", plan.ID, operator,
		fmt.Sprintf("isolation plan drafted for section %s", sectionID), map[string]any{
			"section_id": sectionID, "zone_id": section.ZoneID,
			"partition_revision": baseline.PartitionRevision,
			"relief_valve":       section.ReliefValve, "boundary_valves": valves,
		})
	return plan.Clone(), nil
}

// Start 把草案置为运行中并执行第一个步骤（开泄压阀）。
func (s *PlanService) Start(ctx context.Context, planID, actor string) (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, err := s.load(planID, actor)
	if err != nil {
		return Plan{}, err
	}
	if plan.Status != PlanDraft {
		return plan, fmt.Errorf("plan %s is %s", planID, plan.Status)
	}
	if reason := s.guard(&plan); reason != nil {
		return s.freeze(&plan, reason)
	}
	plan.Status = PlanRunning
	plan.StartedAt = time.Now().UTC()
	if _, err := s.state.Update(plan.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolating
		return nil
	}); err != nil {
		return s.fail(&plan, err)
	}
	if err := s.runStep(ctx, &plan, 0); err != nil {
		return s.fail(&plan, err)
	}
	return s.afterStep(&plan)
}

// Advance 执行下一个未完成步骤。运行条件（分区修订、压力、保护状态）变化时冻结后续动作。
func (s *PlanService) Advance(ctx context.Context, planID, actor string) (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, err := s.load(planID, actor)
	if err != nil {
		return Plan{}, err
	}
	if plan.Status != PlanRunning {
		return plan, fmt.Errorf("plan %s is %s", planID, plan.Status)
	}
	if reason := s.guard(&plan); reason != nil {
		return s.freeze(&plan, reason)
	}
	index := s.nextStep(&plan)
	if index < 0 {
		return plan, fmt.Errorf("plan %s has no pending step", planID)
	}
	if err := s.runStep(ctx, &plan, index); err != nil {
		return s.fail(&plan, err)
	}
	return s.afterStep(&plan)
}

// Resume 在人工确认现场安全后解除可恢复的冻结（压力漂移、保护状态变化、验证失败），
// 以当前压力与保护状态重建基线；分区修订漂移不可恢复。
func (s *PlanService) Resume(planID, actor string) (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, err := s.load(planID, actor)
	if err != nil {
		return Plan{}, err
	}
	if plan.Status != PlanFrozen || plan.Freeze == nil {
		return plan, fmt.Errorf("plan %s is not frozen", planID)
	}
	if !plan.Freeze.Resumable {
		return plan, fmt.Errorf("plan %s bound to stale partition revision, abort and create a new plan", planID)
	}
	zone, ok := s.state.Get(plan.ZoneID)
	if !ok {
		return plan, fmt.Errorf("zone %s not found", plan.ZoneID)
	}
	plan.Baseline.SupplyPressure = zone.SupplyPressure
	plan.Baseline.ReturnPressure = zone.ReturnPressure
	plan.Baseline.Incidents = s.activeIncidents(plan.ZoneID)
	plan.Freeze = nil
	plan.Status = PlanRunning
	s.store(&plan)
	_ = s.audit.Accepted("isolation_plan.resume", plan.ID, actor, "operator acknowledged conditions and resumed plan", map[string]any{
		"section_id": plan.SectionID, "supply_pressure": zone.SupplyPressure, "return_pressure": zone.ReturnPressure,
	})
	return plan.Clone(), nil
}

// Abort 作废计划。已动作的阀门保持现场位置，由操作人按现场规程处置。
func (s *PlanService) Abort(planID, actor, reason string) (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, err := s.load(planID, actor)
	if err != nil {
		return Plan{}, err
	}
	if plan.Status == PlanCompleted || plan.Status == PlanAborted {
		return plan, fmt.Errorf("plan %s is %s", planID, plan.Status)
	}
	plan.Status = PlanAborted
	plan.CompletedAt = time.Now().UTC()
	if plan.Freeze == nil {
		plan.Freeze = &FreezeReason{Kind: "aborted", Message: reason, Resumable: false, At: time.Now().UTC()}
	}
	s.store(&plan)
	_ = s.audit.Rejected("isolation_plan.abort", plan.ID, actor, reason, map[string]any{"section_id": plan.SectionID})
	return plan.Clone(), nil
}

func (s *PlanService) Get(planID string) (Plan, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, ok := s.plans[planID]
	return plan.Clone(), ok
}

func (s *PlanService) List() []Plan {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Plan, 0, len(s.plans))
	for _, plan := range s.plans {
		result = append(result, plan.Clone())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (s *PlanService) load(planID, actor string) (Plan, error) {
	plan, ok := s.plans[planID]
	if !ok {
		return Plan{}, fmt.Errorf("isolation plan %s not found", planID)
	}
	if actor != plan.Operator {
		return Plan{}, fmt.Errorf("plan %s is bound to operator %s", planID, plan.Operator)
	}
	return plan, nil
}

func (s *PlanService) nextStep(plan *Plan) int {
	for i := range plan.Steps {
		if plan.Steps[i].Status != StepDone {
			return i
		}
	}
	return -1
}

func (s *PlanService) heldRelief(plan *Plan) valve.ReliefProof {
	for i := range plan.Steps {
		if plan.Steps[i].Kind == StepReliefOpen && plan.Steps[i].Relief != nil {
			return *plan.Steps[i].Relief
		}
	}
	return valve.ReliefProof{}
}

// runStep 执行单个步骤。调用方必须持有 s.mu。
func (s *PlanService) runStep(ctx context.Context, plan *Plan, index int) error {
	step := &plan.Steps[index]
	held := s.heldRelief(plan)
	switch step.Kind {
	case StepReliefOpen:
		proof, err := s.relief.Open(ctx, step.ValveID, plan.ID)
		if err != nil {
			return err
		}
		step.Relief = &proof
	case StepBoundaryClose:
		proof, err := s.boundaries.Close(ctx, step.ValveID, plan.ID, held)
		if err != nil {
			return err
		}
		step.Boundary = &proof
	case StepVerify:
		proof := VerificationProof{
			BoundariesClosed: make(map[string]bool, len(plan.BoundaryValves)),
			At:               time.Now().UTC(),
		}
		reliefOpen := s.relief.IsOpen(plan.ReliefValve)
		proof.ReliefOpen = reliefOpen
		allClosed := reliefOpen
		for _, valveID := range plan.BoundaryValves {
			closed := s.boundaries.IsClosed(valveID)
			proof.BoundariesClosed[valveID] = closed
			allClosed = allClosed && closed
		}
		step.Verify = &proof
		if !allClosed {
			return fmt.Errorf("isolation verification failed: relief open=%v, boundaries=%v", reliefOpen, proof.BoundariesClosed)
		}
		if _, err := s.state.Update(plan.ZoneID, func(zone *network.Zone) error {
			zone.Phase = network.PhaseIsolated
			return nil
		}); err != nil {
			return err
		}
	case StepBoundaryOpen:
		proof, err := s.boundaries.Open(ctx, step.ValveID, plan.ID, held)
		if err != nil {
			return err
		}
		step.Boundary = &proof
		if _, err := s.state.Update(plan.ZoneID, func(zone *network.Zone) error {
			zone.Phase = network.PhaseRecovering
			return nil
		}); err != nil {
			return err
		}
	case StepReliefClose:
		proof, err := s.relief.Close(ctx, step.ValveID, plan.ID, held)
		if err != nil {
			return err
		}
		step.Relief = &proof
	}
	step.Status = StepDone
	step.DoneAt = time.Now().UTC()
	_ = s.audit.Accepted("isolation_plan.step", plan.ID, plan.Operator, step.Label, map[string]any{
		"section_id": plan.SectionID, "sequence": step.Sequence, "kind": string(step.Kind), "valve_id": step.ValveID,
	})
	return nil
}

// afterStep 在步骤完成后复查运行条件；全部步骤完成则收尾，否则因外部变化冻结。调用方持有 s.mu。
func (s *PlanService) afterStep(plan *Plan) (Plan, error) {
	if s.nextStep(plan) < 0 {
		plan.Status = PlanCompleted
		plan.CompletedAt = time.Now().UTC()
		if _, err := s.state.Update(plan.ZoneID, func(zone *network.Zone) error {
			zone.Phase = network.PhaseActive
			return nil
		}); err != nil {
			return s.fail(plan, err)
		}
		s.store(plan)
		_ = s.audit.Accepted("isolation_plan.complete", plan.ID, plan.Operator, "section isolated, verified and restored", map[string]any{"section_id": plan.SectionID})
		return plan.Clone(), nil
	}
	if reason := s.guard(plan); reason != nil {
		return s.freeze(plan, reason)
	}
	s.store(plan)
	return plan.Clone(), nil
}

// guard 对照绑定基线检查分区修订、压力与保护状态。调用方持有 s.mu。
func (s *PlanService) guard(plan *Plan) *FreezeReason {
	zone, ok := s.state.Get(plan.ZoneID)
	if !ok {
		return &FreezeReason{Kind: FreezePressureDrift, Message: fmt.Sprintf("zone %s vanished", plan.ZoneID), Resumable: false, At: time.Now().UTC()}
	}
	if zone.PartitionRevision != plan.Baseline.PartitionRevision {
		return &FreezeReason{
			Kind: FreezePartitionRevision, Resumable: false, At: time.Now().UTC(),
			Message: fmt.Sprintf("partition revision changed %d -> %d", plan.Baseline.PartitionRevision, zone.PartitionRevision),
		}
	}
	if math.Abs(zone.SupplyPressure-plan.Baseline.SupplyPressure) > s.tolerance ||
		math.Abs(zone.ReturnPressure-plan.Baseline.ReturnPressure) > s.tolerance {
		return &FreezeReason{
			Kind: FreezePressureDrift, Resumable: true, At: time.Now().UTC(),
			Message: fmt.Sprintf("pressure drifted beyond %.2f (supply %.2f->%.2f, return %.2f->%.2f)",
				s.tolerance, plan.Baseline.SupplyPressure, zone.SupplyPressure, plan.Baseline.ReturnPressure, zone.ReturnPressure),
		}
	}
	current := s.activeIncidents(plan.ZoneID)
	if !sameStringSet(current, plan.Baseline.Incidents) {
		return &FreezeReason{
			Kind: FreezeProtection, Resumable: true, At: time.Now().UTC(),
			Message: fmt.Sprintf("active protection incidents changed: baseline %v, now %v", plan.Baseline.Incidents, current),
		}
	}
	return nil
}

func (s *PlanService) freeze(plan *Plan, reason *FreezeReason) (Plan, error) {
	plan.Status = PlanFrozen
	plan.Freeze = reason
	s.store(plan)
	_ = s.audit.Rejected("isolation_plan.freeze", plan.ID, plan.Operator, reason.Message, map[string]any{
		"section_id": plan.SectionID, "reason": reason.Kind, "resumable": reason.Resumable,
	})
	return plan.Clone(), fmt.Errorf("isolation plan %s frozen: %s", plan.ID, reason.Message)
}

func (s *PlanService) fail(plan *Plan, cause error) (Plan, error) {
	reason := &FreezeReason{Kind: FreezeVerification, Message: cause.Error(), Resumable: true, At: time.Now().UTC()}
	return s.freeze(plan, reason)
}

func (s *PlanService) store(plan *Plan) {
	s.plans[plan.ID] = plan.Clone()
	if plan.Status == PlanCompleted || plan.Status == PlanAborted {
		delete(s.activeSect, plan.SectionID)
	} else {
		s.activeSect[plan.SectionID] = plan.ID
	}
}

// activeIncidents 返回分区内各站当前活动保护事件的有序 ID 快照。
func (s *PlanService) activeIncidents(zoneID string) []string {
	if s.protection == nil {
		return nil
	}
	inZone := make(map[string]bool)
	for _, binding := range s.registry.ForZone(zoneID) {
		inZone[binding.StationID] = true
	}
	ids := make([]string, 0)
	for _, incident := range s.protection.List() {
		if incident.Active && inZone[incident.StationID] {
			ids = append(ids, incident.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func sameStringSet(a, b []string) bool {
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
