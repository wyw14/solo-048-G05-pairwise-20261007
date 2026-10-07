package isolation

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/network"
	"heatpulse/internal/protection"
	"heatpulse/internal/store"
	"heatpulse/internal/valve"
)

type harness struct {
	state      *network.State
	topology   *network.Topology
	registry   *network.Registry
	relief     *valve.ReliefService
	boundaries *valve.BoundaryService
	protection *protection.Service
	service    *PlanService
}

func newHarness(t *testing.T) harness {
	t.Helper()
	state := network.NewState([]network.Zone{
		{ID: "east", Name: "东支线", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.76, ReturnPressure: 0.45},
	})
	topology := network.NewTopology([]network.Section{
		{ID: "east-branch", ZoneID: "east", Upstream: "east-supply", Downstream: "east-return",
			BoundaryValves: []string{"east-bv-a", "east-bv-b"}, ReliefValve: "east-rv"},
	})
	registry := network.NewRegistry([]network.StationBinding{{StationID: "binhe", ZoneID: "east", Required: true}})
	relief := valve.NewReliefService(0)
	boundaries := valve.NewBoundaryService()
	protectionService := protection.NewService()
	log := store.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl"))
	service := NewPlanService(topology, state, registry, relief, boundaries, protectionService, audit.NewRecorder(log), 0.05)
	return harness{state, topology, registry, relief, boundaries, protectionService, service}
}

func runToCompletion(t *testing.T, h harness, planID string) Plan {
	t.Helper()
	plan, ok := h.service.Get(planID)
	if !ok {
		t.Fatalf("plan %s not found", planID)
	}
	if plan.Status == PlanDraft {
		var err error
		plan, err = h.service.Start(context.Background(), planID, "operator-li")
		if err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	var err error
	for plan.Status == PlanRunning {
		plan, err = h.service.Advance(context.Background(), planID, "operator-li")
		if err != nil && plan.Status != PlanFrozen {
			t.Fatalf("advance: %v", err)
		}
	}
	return plan
}

func TestCreateBuildsOrderedStepsAndBindsRevision(t *testing.T) {
	h := newHarness(t)
	plan, err := h.service.Create("east-branch", "operator-li")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if plan.Status != PlanDraft || plan.Operator != "operator-li" {
		t.Fatalf("unexpected draft binding: %+v", plan)
	}
	if plan.Baseline.PartitionRevision != 0 {
		t.Fatalf("expected partition revision 0, got %d", plan.Baseline.PartitionRevision)
	}
	wantKinds := []StepKind{
		StepReliefOpen,
		StepBoundaryClose, StepBoundaryClose,
		StepVerify,
		StepBoundaryOpen, StepBoundaryOpen,
		StepReliefClose,
	}
	if len(plan.Steps) != len(wantKinds) {
		t.Fatalf("got %d steps, want %d", len(plan.Steps), len(wantKinds))
	}
	for i, want := range wantKinds {
		if plan.Steps[i].Kind != want || plan.Steps[i].Sequence != i+1 {
			t.Fatalf("step %d = %s (seq %d), want %s", i, plan.Steps[i].Kind, plan.Steps[i].Sequence, want)
		}
	}
	// 关阀按拓扑顺序，恢复按相反顺序重开。
	if plan.Steps[1].ValveID != "east-bv-a" || plan.Steps[2].ValveID != "east-bv-b" {
		t.Fatalf("boundary close order wrong: %s, %s", plan.Steps[1].ValveID, plan.Steps[2].ValveID)
	}
	if plan.Steps[4].ValveID != "east-bv-b" || plan.Steps[5].ValveID != "east-bv-a" {
		t.Fatalf("boundary reopen order should be reversed: %s, %s", plan.Steps[4].ValveID, plan.Steps[5].ValveID)
	}
	if plan.ReliefValve != "east-rv" {
		t.Fatalf("relief valve selection wrong: %s", plan.ReliefValve)
	}

	if _, err := h.service.Create("east-branch", "operator-li"); err == nil {
		t.Fatal("expected duplicate active plan to be rejected")
	}
	if _, err := h.service.Create("east-branch", ""); err == nil {
		t.Fatal("expected plan without operator to be rejected")
	}
	if _, err := h.service.Create("missing", "operator-li"); err == nil {
		t.Fatal("expected unknown section to be rejected")
	}
}

func TestHappyPathCompletesAndRestoresDevices(t *testing.T) {
	h := newHarness(t)
	plan, err := h.service.Create("east-branch", "operator-li")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	final := runToCompletion(t, h, plan.ID)
	if final.Status != PlanCompleted {
		t.Fatalf("status = %s, freeze = %+v", final.Status, final.Freeze)
	}
	for _, step := range final.Steps {
		if step.Status != StepDone {
			t.Fatalf("step %d not done: %s", step.Sequence, step.Kind)
		}
	}
	if h.relief.IsOpen("east-rv") {
		t.Fatal("relief valve should be closed after restore")
	}
	if h.boundaries.IsClosed("east-bv-a") || h.boundaries.IsClosed("east-bv-b") {
		t.Fatal("boundary valves should be reopened after restore")
	}
	verify := final.Steps[3].Verify
	if verify == nil || !verify.ReliefOpen || !verify.BoundariesClosed["east-bv-a"] || !verify.BoundariesClosed["east-bv-b"] {
		t.Fatalf("verification proof incomplete: %+v", verify)
	}
	zone, _ := h.state.Get("east")
	if zone.Phase != network.PhaseActive {
		t.Fatalf("zone phase = %s, want active", zone.Phase)
	}
}

func TestPressureDriftFreezesAndResumeRebases(t *testing.T) {
	h := newHarness(t)
	plan, _ := h.service.Create("east-branch", "operator-li")
	if _, err := h.service.Start(context.Background(), plan.ID, "operator-li"); err != nil {
		t.Fatalf("start: %v", err)
	}
	// 运行中供水压力超出容差，后续动作必须冻结。
	if _, err := h.state.Update("east", func(zone *network.Zone) error {
		zone.SupplyPressure = 1.10
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	frozen, err := h.service.Advance(context.Background(), plan.ID, "operator-li")
	if err == nil || frozen.Status != PlanFrozen {
		t.Fatalf("expected frozen plan, got status %s err %v", frozen.Status, err)
	}
	if frozen.Freeze.Kind != FreezePressureDrift || !frozen.Freeze.Resumable {
		t.Fatalf("freeze reason = %+v", frozen.Freeze)
	}
	// 冻结期间不得执行任何关阀动作。
	if h.boundaries.IsClosed("east-bv-a") {
		t.Fatal("boundary must not be closed while frozen")
	}
	// 非绑定操作人无权解除冻结。
	if _, err := h.service.Resume(plan.ID, "intruder"); err == nil {
		t.Fatal("resume by another operator must be rejected")
	}
	resumed, err := h.service.Resume(plan.ID, "operator-li")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Status != PlanRunning || resumed.Baseline.SupplyPressure != 1.10 {
		t.Fatalf("resume did not rebase baseline: %+v", resumed.Baseline)
	}
	final := runToCompletion(t, h, plan.ID)
	if final.Status != PlanCompleted {
		t.Fatalf("status after resume = %s: %+v", final.Status, final.Freeze)
	}
}

func TestProtectionStateChangeFreezes(t *testing.T) {
	h := newHarness(t)
	plan, _ := h.service.Create("east-branch", "operator-li")
	if _, err := h.service.Start(context.Background(), plan.ID, "operator-li"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.protection.Raise("binhe", "pressure_trip", "high", "trip during isolation"); err != nil {
		t.Fatal(err)
	}
	frozen, err := h.service.Advance(context.Background(), plan.ID, "operator-li")
	if err == nil || frozen.Status != PlanFrozen || frozen.Freeze.Kind != FreezeProtection {
		t.Fatalf("expected protection freeze, got %s %+v err %v", frozen.Status, frozen.Freeze, err)
	}
	if !frozen.Freeze.Resumable {
		t.Fatal("protection freeze should be resumable after operator acknowledgement")
	}
}

func TestPartitionRevisionChangeFreezesIrrecoverably(t *testing.T) {
	h := newHarness(t)
	plan, _ := h.service.Create("east-branch", "operator-li")
	if _, err := h.service.Start(context.Background(), plan.ID, "operator-li"); err != nil {
		t.Fatalf("start: %v", err)
	}
	bumped, err := h.state.BumpPartitionRevision("east")
	if err != nil {
		t.Fatal(err)
	}
	if bumped.PartitionRevision != 1 {
		t.Fatalf("revision = %d", bumped.PartitionRevision)
	}
	frozen, err := h.service.Advance(context.Background(), plan.ID, "operator-li")
	if err == nil || frozen.Status != PlanFrozen {
		t.Fatalf("expected frozen, got %s err %v", frozen.Status, err)
	}
	if frozen.Freeze.Kind != FreezePartitionRevision || frozen.Freeze.Resumable {
		t.Fatalf("partition freeze must be non-resumable: %+v", frozen.Freeze)
	}
	if _, err := h.service.Resume(plan.ID, "operator-li"); err == nil {
		t.Fatal("stale partition revision must not allow resume")
	}
	aborted, err := h.service.Abort(plan.ID, "operator-li", "partition republished")
	if err != nil || aborted.Status != PlanAborted {
		t.Fatalf("abort: %v status %s", err, aborted.Status)
	}
	// 旧计划作废后可基于新分区修订重新编计划。
	replan, err := h.service.Create("east-branch", "operator-li")
	if err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if replan.Baseline.PartitionRevision != 1 {
		t.Fatalf("new plan should bind revision 1, got %d", replan.Baseline.PartitionRevision)
	}
}

func TestVerificationFailureFreezes(t *testing.T) {
	h := newHarness(t)
	plan, _ := h.service.Create("east-branch", "operator-li")
	if _, err := h.service.Start(context.Background(), plan.ID, "operator-li"); err != nil {
		t.Fatalf("start: %v", err)
	}
	// 第一步后东 a 已关；推进关闭东 b 之前，用另一会话把东 a 重新打开，模拟阀位失效。
	if _, err := h.service.Advance(context.Background(), plan.ID, "operator-li"); err != nil {
		t.Fatalf("advance close a: %v", err)
	}
	held := valve.ReliefProof{ValveID: "east-rv", SessionID: "other-session", Open: true}
	if _, err := h.boundaries.Open(context.Background(), "east-bv-a", "other-session", held); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := h.service.Advance(context.Background(), plan.ID, "operator-li"); err != nil { // close b
		t.Fatalf("advance close b: %v", err)
	}
	frozen, err := h.service.Advance(context.Background(), plan.ID, "operator-li") // verify
	if err == nil || frozen.Status != PlanFrozen {
		t.Fatalf("expected verification freeze, got %s err %v", frozen.Status, err)
	}
	if !strings.Contains(frozen.Freeze.Message, "verification failed") {
		t.Fatalf("unexpected freeze message: %s", frozen.Freeze.Message)
	}
	if frozen.Steps[3].Status == StepDone {
		t.Fatal("verify step must remain pending after failure")
	}
}
