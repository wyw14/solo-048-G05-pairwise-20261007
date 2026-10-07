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

// Open 在分区恢复时重新打开边界阀。泄压通路必须仍由同一计划会话持有，
// 确保重新充压过程中异常压力有泄放路径；随后再由恢复末步关闭泄压阀。
func (s *BoundaryService) Open(ctx context.Context, valveID, sessionID string, relief ReliefProof) (BoundaryProof, error) {
	if err := ctx.Err(); err != nil {
		return BoundaryProof{}, err
	}
	if !relief.Open || relief.SessionID != sessionID {
		return BoundaryProof{}, fmt.Errorf("boundary reopening requires an open relief proof")
	}
	proof := BoundaryProof{ValveID: valveID, SessionID: sessionID, Closed: false, At: time.Now().UTC()}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed[valveID] = false
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
