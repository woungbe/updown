package service

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"woungbe/updown_0.1/mybinance"

	"github.com/spf13/viper"
)

/*
필요한 데이터 만들기

symbol
금액 - 벨런스의 몇 %
익절 금액
횟수
*/
type UpDownAlgorithm struct {
	Symbol     string  // 심볼
	Amount     float64 // 금액 USDT 기준
	TakeProfit float64 // 익절 퍼센트
	Interval   int     // LONG, SHORT 간격
	Multiplier float64 //  1 ~ 4 배씩
	Count      int     // 횟수

	acckey string
	seckey string

	socket  *mybinance.BinanceUserDataStream
	binance *mybinance.BinanceUser
}

func (ty *UpDownAlgorithm) getKey(env string) (string, string) {
	viper.SetConfigFile(env) // .env 파일 설정
	viper.AutomaticEnv()     // 환경 변수를 자동으로 읽도록 설정
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	accKey := viper.GetString("AccessKey")
	seckey := viper.GetString("SecretKey")
	return accKey, seckey
}

// 초기화
func (ty *UpDownAlgorithm) InitService() *UpDownAlgorithm {
	ty.acckey, ty.seckey = ty.getKey("../.dev.env")
	ty.initSocket()

	return ty
}

// 설정하기
func (ty *UpDownAlgorithm) Setting(Symbol string, Amount float64, TakeProfit float64, Interval int, Multiplier float64, Count int) {
	ty.Symbol = Symbol
	ty.Amount = Amount
	ty.TakeProfit = TakeProfit
	ty.Interval = Interval
	ty.Multiplier = Multiplier
	ty.Count = Count
}

// 실행
func (ty *UpDownAlgorithm) Run() {

}

// 소켓 초기화
func (ty *UpDownAlgorithm) initSocket() {

	binanceUser := new(mybinance.BinanceUser)
	binanceUser.Init(ty.acckey, ty.seckey)

	listenKey, err := binanceUser.GetStartUserStreamService()
	if err != nil {
		log.Fatalf("Error getting listen key: %v", err)
	}

	userStream := mybinance.BinanceUserDataStream{}
	userStream.Init(ty.acckey)

	// WebSocket 데이터를 받을 채널 생성
	dataChannel := make(chan []byte)

	// listenKey = "BLYYivLtFECS3FM8CTQETVElTLuSMHhvbWdPmos513Pd5tCdyh23sFd81ct3BhmL"

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
		}
	}
}
