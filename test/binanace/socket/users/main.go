package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"woungbe/updown_0.1/mybinance"
)

func main() {
	// API 키와 Secret Key를 가져옴
	apiKey, secKey := mybinance.GetKey()
	fmt.Println(apiKey, secKey)

	// BinanceUser 인스턴스 생성 및 초기화
	binanceUser := new(mybinance.BinanceUser)
	binanceUser.Init(apiKey, secKey)

	// 유저 스트림 Listen Key를 얻음
	listenKey, err := binanceUser.GetStartUserStreamService()
	if err != nil {
		log.Fatalf("Error getting listen key: %v", err)
	}
	fmt.Printf("Listen Key: %s\n", listenKey)

	// BinanceUserDataStream 인스턴스 생성 및 초기화
	userStream := mybinance.BinanceUserDataStream{}
	userStream.Init(apiKey)

	// WebSocket 데이터를 받을 채널 생성
	dataChannel := make(chan []byte)

	// WebSocket 연결
	err = userStream.ConnectUserDataWebSocket(listenKey, dataChannel)
	if err != nil {
		log.Fatalf("Failed to connect WebSocket: %v", err)
	}
	log.Println("Successfully connected to WebSocket")

	// 종료 시그널 처리를 위한 채널
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// WebSocket 데이터 처리
	for {
		select {
		case data := <-dataChannel:
			if len(data) > 0 {
				fmt.Printf("Received data: %s\n", string(data))
			}
		case <-sigChan:
			log.Println("Shutting down gracefully...")
			return
		}
	}
}
