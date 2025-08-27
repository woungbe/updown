package main

import (
	"fmt"
	"woungbe/updown_0.1/mybinance"
)

func main() {
	// BinanceSocketTicker 인스턴스 생성
	ticker := &mybinance.BinanceSocketTicker{}
	ticker.Init()

	// WebSocket 연결
	ticker.ConnectWebSocket()

	// 심볼 구독 (BTCUSDT)
	ticker.Subscribe("btcusdt")

	// 메시지를 비동기로 수신하고 처리
	go ticker.ReceiveMessages(func(message []byte) {
		// 수신된 메시지를 처리하는 로직
		fmt.Printf("Received message: %s\n", message)
	})

	// 프로그램이 종료되지 않도록 기다림
	select {}
}
