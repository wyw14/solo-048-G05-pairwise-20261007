package valve

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ReliefProof struct {
	ValveID   string    `json:"valve_id"`
	SessionID string    `json:"session_id"`
	Open      bool      `json:"open"`
	At        time.Time `json:"at"`
}

type ReliefService struct {
	mu       sync.RWMutex
	position map[string]bool
	delay    time.Duration
}

func NewReliefService(delay time.Duration) *ReliefService {
	return &ReliefService{position: make(map[string]bool), delay: delay}
}

func (s *ReliefService) Open(ctx context.Context, valveID, sessionID string) (ReliefProof, error) {
	if s.delay < 0 {
		return ReliefProof{}, fmt.Errorf("relief valve %s driver unavailable", valveID)
	}
	select {
	case <-ctx.Done():
		return ReliefProof{}, ctx.Err()
	case <-time.After(s.delay):
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.position[valveID] = true
	return ReliefProof{ValveID: valveID, SessionID: sessionID, Open: true, At: time.Now().UTC()}, nil
}

// Close 在分区恢复完成后复位泄压阀。仅允许关闭当前由同一计划会话打开的泄压通路，
// 避免误关他人持有的泄压阀。
func (s *ReliefService) Close(ctx context.Context, valveID, sessionID string, held ReliefProof) (ReliefProof, error) {
	if err := ctx.Err(); err != nil {
		return ReliefProof{}, err
	}
	if !held.Open || held.ValveID != valveID || held.SessionID != sessionID {
		return ReliefProof{}, fmt.Errorf("relief closure requires current relief proof")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.position[valveID] {
		return ReliefProof{}, fmt.Errorf("relief valve %s already closed", valveID)
	}
	s.position[valveID] = false
	return ReliefProof{ValveID: valveID, SessionID: sessionID, Open: false, At: time.Now().UTC()}, nil
}

func (s *ReliefService) IsOpen(valveID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position[valveID]
}
