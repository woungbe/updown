package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
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
	url := "https://api.binance.com/api/v3/userDataStream"

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


func main() {
	// === 설정 ===
	err := godotenv.Load()
	if err != nil {
		log.Println(".env 파일을 찾을 수 없거나 로드 실패:", err)
	}


	// 예시: 공개 시세는 BTCUSDT, ETHUSDT 구독(Spot)
	symbols := []string{"BTCUSDT"}
	isFutures := false // 선물로 바꾸려면 true

	apiKey := os.Getenv("BINANCE_API_KEY")
	// apiSecret := os.Getenv("BINANCE_API_SECRET")

	if apiKey == "" {
		log.Fatal("BINANCE_API_KEY 가 필요합니다")
	}

	// listenKey 발급
	listenKey, err := getListenKey(apiKey)
	if err != nil {
		log.Fatal("listenKey 발급 에러:", err)
	}


	httpAddr := ":8080"
	hub := NewHub()

	// 샘플: 내부 구독자 하나 붙여서 로그로 확인
	priceCh, cancelPrice := hub.SubscribePrice()
	defer cancelPrice()
	go func() {
		for e := range priceCh {
			log.Printf("[PRICE] %s bid=%.2f ask=%.2f t=%s", e.Symbol, e.BidPrice, e.AskPrice, e.RecvTime.Format(time.RFC3339))
		}
	}()

	userCh, cancelUser := hub.SubscribeUser()
	defer cancelUser()
	go func() {
		for e := range userCh {
			if e.EventType != "" {
				log.Printf("[USER] %s at %d, sym=%s status=%s exec=%s",
					e.EventType, e.EventTime, e.Order.Symbol, e.Order.Status, e.Order.ExecutionType)
			} else {
				log.Printf("[USER] raw=%s", string(e.Raw))
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 공개 WS
	go func() {
		for {
			if err := runPublicWS(ctx, hub, isFutures, symbols); err != nil {
				log.Printf("[public] error: %v (reconnect in 2s)", err)
				time.Sleep(2 * time.Second)
				continue
			}
			return
		}
	}()

	// 유저 WS (listenKey 있을 때만)
	if listenKey != "" {
		go func() {
			for {
				if err := runUserWS(ctx, hub, isFutures, listenKey); err != nil {
					log.Printf("[user] error: %v (reconnect in 2s)", err)
					time.Sleep(2 * time.Second)
					continue
				}
				return
			}
		}()
	}

	// HTTP: 스냅샷 조회
	http.HandleFunc("/price/", priceHandler(hub))
	log.Printf("HTTP listen on %s", httpAddr)
	log.Fatal(http.ListenAndServe(httpAddr, nil))
}
