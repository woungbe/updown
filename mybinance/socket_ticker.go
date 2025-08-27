package mybinance

import (
	"log"

	"github.com/gorilla/websocket"
)

const binanceFuturesURL = "wss://fstream.binance.com/ws"

type BinanceSocketTicker struct {
	binanceFuturesURL string
	c                 *websocket.Conn // 웹소켓 연결
	done              chan struct{}   // 웹소켓 종료 채널
	messageChannel    chan []byte     // 메시지를 전달할 채널
}

// Init initializes the BinanceSocketTicker
func (ty *BinanceSocketTicker) Init() {
	ty.binanceFuturesURL = binanceFuturesURL
	ty.done = make(chan struct{})         // 웹소켓 종료 채널
	ty.messageChannel = make(chan []byte) // 메시지를 전달할 채널
}

// WebSocket 연결을 생성하고 데이터를 수신하는 함수
func (ty *BinanceSocketTicker) ConnectWebSocket() {
	var err error
	log.Printf("Connecting to %s", binanceFuturesURL)

	ty.c, _, err = websocket.DefaultDialer.Dial(binanceFuturesURL, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	// 데이터 수신을 위한 고루틴
	go func() {
		defer close(ty.done)
		for {
			_, message, err := ty.c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			// 수신된 메시지를 채널로 전달
			ty.messageChannel <- message
		}
	}()
}

// 특정 심볼을 구독하는 함수
func (ty *BinanceSocketTicker) Subscribe(symbol string) {
	subscribeMessage := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": []string{
			symbol + "@ticker",
		},
		"id": 1,
	}

	err := ty.c.WriteJSON(subscribeMessage)
	if err != nil {
		log.Printf("subscribe error: %v", err)
	}
}

// 특정 심볼 구독을 취소하는 함수
func (ty *BinanceSocketTicker) Unsubscribe(symbol string) {
	unsubscribeMessage := map[string]interface{}{
		"method": "UNSUBSCRIBE",
		"params": []string{
			symbol + "@ticker",
		},
		"id": 2,
	}

	err := ty.c.WriteJSON(unsubscribeMessage)
	if err != nil {
		log.Printf("unsubscribe error: %v", err)
	}
}

// 외부에서 메시지 채널을 통해 수신된 메시지를 처리하는 함수
func (ty *BinanceSocketTicker) ReceiveMessages(handler func(message []byte)) {
	for {
		select {
		case msg := <-ty.messageChannel:
			// 메시지를 받아서 콜백 함수로 전달
			handler(msg)
		case <-ty.done:
			return
		}
	}
}
