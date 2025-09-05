package pricecache

import (
	"sync"
	"time"
)

type Tick struct {
	Bid      float64
	Ask      float64
	BidQty   float64
	AskQty   float64
	RecvTime time.Time
}

type Store struct {
	mu sync.RWMutex
	m  map[string]Tick // KEY: 대문자 심볼 ("BTCUSDT")
}

func NewStore() *Store {
	return &Store{m: make(map[string]Tick)}
}

// 최신값 덮어쓰기
func (s *Store) Set(symbol string, t Tick) {
	s.mu.Lock()
	s.m[symbol] = t
	s.mu.Unlock()
}

// 최신값 조회
func (s *Store) Get(symbol string) (Tick, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[symbol]
	return v, ok
}

// 여러 개 한 번에 조회(옵션)
func (s *Store) GetMany(symbols ...string) map[string]Tick {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Tick, len(symbols))
	for _, sym := range symbols {
		if v, ok := s.m[sym]; ok {
			out[sym] = v
		}
	}
	return out
}