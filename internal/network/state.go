package network

import (
	"fmt"
	"sync"
	"time"
)

type Phase string

const (
	PhaseOffline    Phase = "offline"
	PhaseWarming    Phase = "warming"
	PhaseActive     Phase = "active"
	PhaseIsolating  Phase = "isolating"
	PhaseIsolated   Phase = "isolated"
	PhaseRecovering Phase = "recovering"
)

type Zone struct {
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	Phase            Phase   `json:"phase"`
	PumpStage        int     `json:"pump_stage"`
	BypassOpen       bool    `json:"bypass_open"`
	SupplyPressure   float64 `json:"supply_pressure"`
	ReturnPressure   float64 `json:"return_pressure"`
	RecoveryRevision uint64  `json:"recovery_revision"`
	// PartitionRevision 标识当前分区（拓扑/边界划分）版本。任何改变分区边界的操作
	// 都会使它递增，从而冻结仍绑定旧修订的隔离计划。
	PartitionRevision uint64    `json:"partition_revision"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func (z Zone) DifferentialPressure() float64 {
	return z.SupplyPressure - z.ReturnPressure
}

type State struct {
	mu    sync.RWMutex
	zones map[string]Zone
}

func NewState(zones []Zone) *State {
	all := make(map[string]Zone, len(zones))
	for _, zone := range zones {
		zone.UpdatedAt = time.Now().UTC()
		all[zone.ID] = zone
	}
	return &State{zones: all}
}

func (s *State) Get(zoneID string) (Zone, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	zone, ok := s.zones[zoneID]
	return zone, ok
}

func (s *State) List() []Zone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Zone, 0, len(s.zones))
	for _, zone := range s.zones {
		result = append(result, zone)
	}
	return result
}

func (s *State) Update(zoneID string, change func(*Zone) error) (Zone, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	zone, ok := s.zones[zoneID]
	if !ok {
		return Zone{}, fmt.Errorf("zone %s not found", zoneID)
	}
	if err := change(&zone); err != nil {
		return Zone{}, err
	}
	zone.UpdatedAt = time.Now().UTC()
	s.zones[zoneID] = zone
	return zone, nil
}

func (s *State) Activate(zoneID string, revision uint64) (Zone, error) {
	return s.Update(zoneID, func(zone *Zone) error {
		if revision != zone.RecoveryRevision {
			return fmt.Errorf("stale recovery revision %d", revision)
		}
		if zone.Phase != PhaseWarming && zone.Phase != PhaseRecovering {
			return fmt.Errorf("zone %s cannot activate from %s", zoneID, zone.Phase)
		}
		zone.Phase = PhaseActive
		return nil
	})
}

// BumpPartitionRevision 在分区边界（拓扑、边界阀划分）发生变化时调用，
// 使绑定旧分区修订的在途隔离计划无法继续推进。返回更新后的分区。
func (s *State) BumpPartitionRevision(zoneID string) (Zone, error) {
	return s.Update(zoneID, func(zone *Zone) error {
		zone.PartitionRevision++
		return nil
	})
}
