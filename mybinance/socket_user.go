package mybinance

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

// BinanceUserDataStream handles the WebSocket connection for user data
type BinanceUserDataStream struct {
	c      *websocket.Conn // 웹소켓 연결
	done   chan struct{}   // 웹소켓 종료 채널
	apiKey string          // Binance API Key
}

// Init initializes the BinanceUserDataStream
func (ud *BinanceUserDataStream) Init(apiKey string) {
	ud.apiKey = apiKey
	ud.done = make(chan struct{})
}

// WebSocket 연결을 생성하고 유저 데이터를 수신하는 함수
func (ud *BinanceUserDataStream) ConnectUserDataWebSocket(listenKey string, dataChannel chan []byte) error {
	var err error
	wsURL := "wss://fstream.binance.com/ws/" + listenKey
	log.Printf("Connecting to %s", wsURL)

	ud.c, _, err = websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	// 데이터 수신을 위한 고루틴
	go func() {
		defer close(ud.done)
		for {
			_, message, err := ud.c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			// 수신된 유저 정보를 출력
			log.Printf("User Data recv: %s", message)
			dataChannel <- message
		}
	}()
	return nil
}

// 유저 스트림 WebSocket 연결 종료
func (ud *BinanceUserDataStream) Close() {
	err := ud.c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		log.Println("write close:", err)
		return
	}
	// Replace select with timer-based channel receive
	timer := time.After(time.Second)
	<-timer
	ud.c.Close()
}
