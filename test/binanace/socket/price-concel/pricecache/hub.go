package pricecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

/*
  === 요약 ===
  - PriceEvent: 공개 시세(bookTicker) 이벤트
  - UserEvent : 계정(User Data Stream) 이벤트(주문/포지션 등)
  - Hub       : 내부 pub/sub, 최신 스냅샷 저장, HTTP 노출
*/

// ===== 모델 =====

type PriceEvent struct {
	Symbol   string  `json:"s"`
	BidPrice float64 `json:"b,string"`
	AskPrice float64 `json:"a,string"`
	BidQty   float64 `json:"B,string"`
	AskQty   float64 `json:"A,string"`
	// 필요시 추가 필드…
	RecvTime time.Time `json:"-"`
}

type UserEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	// 예: 주문 업데이트(ORDER_TRADE_UPDATE)
	// 필요한 최소 필드만 샘플로 파싱(원한다면 구조 확장)
	Order struct {
		Symbol        string `json:"s"`
		Side          string `json:"S"`
		Type          string `json:"o"`
		Status        string `json:"X"`
		ExecutionType string `json:"x"`
		OrderID       int64  `json:"i"`
		ClientOrderID string `json:"c"`
		LastFilledQty string `json:"l"`
		LastFilledPx  string `json:"L"`
		// … 필요 필드 추가
	} `json:"o"`
	Raw json.RawMessage `json:"-"`
}

// ===== Hub: 채널, 구독자, 스냅샷 =====

type Hub struct {
	priceSubsMu sync.RWMutex
	priceSubs   map[chan PriceEvent]struct{}

	userSubsMu sync.RWMutex
	userSubs   map[chan UserEvent]struct{}

	// 최신 시세 스냅샷(심볼 대문자 고정)
	snapMu  sync.RWMutex
	snapMap map[string]PriceEvent
}

func NewHub() *Hub {
	return &Hub{
		priceSubs: make(map[chan PriceEvent]struct{}),
		userSubs:  make(map[chan UserEvent]struct{}),
		snapMap:   make(map[string]PriceEvent),
	}
}

func (h *Hub) PublishPrice(e PriceEvent) {
	// 스냅샷 저장
	h.snapMu.Lock()
	h.snapMap[strings.ToUpper(e.Symbol)] = e
	h.snapMu.Unlock()

	// 구독자에게 fan-out
	h.priceSubsMu.RLock()
	for ch := range h.priceSubs {
		select {
		case ch <- e:
		default: // 백프레셔: 느린 소비자 보호
			// 드랍하거나 버퍼 늘리기 등 정책 결정
		}
	}
	h.priceSubsMu.RUnlock()
}

func (h *Hub) PublishUser(e UserEvent) {
	h.userSubsMu.RLock()
	for ch := range h.userSubs {
		select {
		case ch <- e:
		default:
		}
	}
	h.userSubsMu.RUnlock()
}

func (h *Hub) SubscribePrice() (<-chan PriceEvent, func()) {
	ch := make(chan PriceEvent, 128)
	h.priceSubsMu.Lock()
	h.priceSubs[ch] = struct{}{}
	h.priceSubsMu.Unlock()
	cancel := func() {
		h.priceSubsMu.Lock()
		delete(h.priceSubs, ch)
		close(ch)
		h.priceSubsMu.Unlock()
	}
	return ch, cancel
}

func (h *Hub) SubscribeUser() (<-chan UserEvent, func()) {
	ch := make(chan UserEvent, 128)
	h.userSubsMu.Lock()
	h.userSubs[ch] = struct{}{}
	h.userSubsMu.Unlock()
	cancel := func() {
		h.userSubsMu.Lock()
		delete(h.userSubs, ch)
		close(ch)
		h.userSubsMu.Unlock()
	}
	return ch, cancel
}

func (h *Hub) Snapshot(symbol string) (PriceEvent, bool) {
	h.snapMu.RLock()
	defer h.snapMu.RUnlock()
	pe, ok := h.snapMap[strings.ToUpper(symbol)]
	return pe, ok
}

// ===== Binance WebSocket 클라이언트 =====

// 공개 시세(bookTicker) — 여러 심볼 구독
// (Spot: wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker)
// (Futures: wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/…)
func runPublicWS(ctx context.Context, hub *Hub, isFutures bool, symbols []string) error {
	if len(symbols) == 0 {
		return errors.New("no symbols provided")
	}
	var base string
	if isFutures {
		base = "wss://fstream.binance.com/stream"
	} else {
		base = "wss://stream.binance.com:9443/stream"
	}
	var streams []string
	for _, s := range symbols {
		streams = append(streams, strings.ToLower(s)+"@bookTicker")
	}
	url := fmt.Sprintf("%s?streams=%s", base, strings.Join(streams, "/"))
	log.Printf("[public] connecting: %s", url)

	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
	c.SetPongHandler(func(string) error {
		_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	// ping 루프
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = c.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
			}
		}
	}()

	// 수신 루프
	type envelope struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_, message, err := c.ReadMessage()
		if err != nil {
			return err
		}
		var env envelope
		if err := json.Unmarshal(message, &env); err != nil {
			continue
		}
		var pe PriceEvent
		if err := json.Unmarshal(env.Data, &pe); err != nil {
			continue
		}
		pe.RecvTime = time.Now()
		hub.PublishPrice(pe)
	}
}

// User Data Stream — listenKey 필요(환경변수 BINANCE_LISTEN_KEY)
// (Spot: wss://stream.binance.com:9443/ws/<listenKey>)
// (Futures: wss://fstream.binance.com/ws/<listenKey>)
func runUserWS(ctx context.Context, hub *Hub, isFutures bool, listenKey string) error {
	if listenKey == "" {
		return errors.New("empty listenKey")
	}
	var url string
	if isFutures {
		url = fmt.Sprintf("wss://fstream.binance.com/ws/%s", listenKey)
	} else {
		url = fmt.Sprintf("wss://stream.binance.com:9443/ws/%s", listenKey)
	}
	log.Printf("[user] connecting: %s", url)

	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
	c.SetPongHandler(func(string) error {
		_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	// ping 루프
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = c.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_, message, err := c.ReadMessage()
		if err != nil {
			return err
		}
		var ue UserEvent
		if err := json.Unmarshal(message, &ue); err != nil {
			// 파싱 안되면 원본만 실어보내기
			ue = UserEvent{Raw: message}
		}
		if ue.EventType == "" {
			ue.Raw = message
		}
		hub.PublishUser(ue)
	}
}

// ===== HTTP 핸들러 =====

func priceHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// /price/BTCUSDT
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 2 {
			http.Error(w, "use /price/{symbol}", http.StatusBadRequest)
			return
		}
		symbol := parts[1]
		if pe, ok := hub.Snapshot(symbol); ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pe)
			return
		}
		http.Error(w, "no snapshot", http.StatusNotFound)
	}
}

// Binance UserDataStream listenKey 응답 구조체
type listenKeyRes struct {
	ListenKey string `json:"listenKey"`
}

// listenKey 발급 함수 (Spot)
func getListenKey(apiKey string) (string, error) {
	url := "https://fapi.binance.com/fapi/v1/listenKey"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte{}))
	if err != nil {
		return "", err
	}
	req.Header.Set("X-MBX-APIKEY", apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("listenKey 발급 실패: %s", string(body))
	}

	var res listenKeyRes
	if err := json.Unmarshal(body, &res); err != nil {
		return "", err
	}

	return res.ListenKey, nil
}
