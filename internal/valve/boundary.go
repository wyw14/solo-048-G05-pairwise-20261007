package valve

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type BoundaryProof struct {
	ValveID   string    `json:"valve_id"`
	SessionID string    `json:"session_id"`
	Closed    bool      `json:"closed"`
	At        time.Time `json:"at"`
}

type BoundaryService struct {
	mu       sync.RWMutex
	closed   map[string]bool
	commands []BoundaryProof
}

func NewBoundaryService() *BoundaryService {
	return &BoundaryService{closed: make(map[string]bool), commands: make([]BoundaryProof, 0)}
}

func (s *BoundaryService) Close(ctx context.Context, valveID, sessionID string, relief ReliefProof) (BoundaryProof, error) {
	if err := ctx.Err(); err != nil {
		return BoundaryProof{}, err
	}
	if !relief.Open || relief.SessionID != sessionID {
		return BoundaryProof{}, fmt.Errorf("boundary closure requires current relief proof")
	}
	proof := BoundaryProof{ValveID: valveID, SessionID: sessionID, Closed: true, At: time.Now().UTC()}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed[valveID] = true
	s.commands = append(s.commands, proof)
	return proof, nil
}

// CloseIsolating records a boundary closure commanded by an isolation plan.
// Plan-driven isolation closes boundaries before depressurizing the section,
// so unlike the leak-response flow no relief proof is required.
func (s *BoundaryService) CloseIsolating(ctx context.Context, valveID, planID string) (BoundaryProof, error) {
	if err := ctx.Err(); err != nil {
		return BoundaryProof{}, err
	}
	proof := BoundaryProof{ValveID: valveID, SessionID: planID, Closed: true, At: time.Now().UTC()}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed[valveID] = true
	s.commands = append(s.commands, proof)
	return proof, nil
}

func (s *BoundaryService) IsClosed(valveID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed[valveID]
}

func (s *BoundaryService) Commands() []BoundaryProof {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]BoundaryProof(nil), s.commands...)
}
