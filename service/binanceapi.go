package service

import (
	"fmt"
	"log"
	"woungbe/updown_0.1/mybinance"

	"github.com/adshao/go-binance/v2/futures"
)

// 소켓 관련 데이터 처리 전부 ////////////////////////////////

// 유저 관련 처리  ////////////////////////////////
func (ty *UpDownAlgorithm) InitBinanceUser(apikey, seckey string) {
	ty.binance = new(mybinance.BinanceUser)
	ty.binance.Init(apikey, seckey)
}

// market 주문
func (ty *UpDownAlgorithm) MarketOrder(Longshort string, Openclose string, Amount string) (*futures.CreateOrderResponse, error) {
	res, err := ty.binance.SendOrderMarket(ty.Symbol, Longshort, Openclose, Amount)
	if err != nil {
		log.Fatalf("Failed to send market order: %v", err)
		return nil, err
	}
	fmt.Printf("Market order response: %v\n", res)

	return res, nil
}

// 익절 주문
func (ty *UpDownAlgorithm) TakeProfitOrder(Longshort string, Openclose string, Price string) (*futures.CreateOrderResponse, error) {
	res, err := ty.binance.SendOrderTakeProfit(ty.Symbol, Longshort, Openclose, Price)
	if err != nil {
		log.Fatalf("Failed to send take profit order: %v", err)
		return nil, err
	}
	fmt.Printf("Take profit order response: %v\n", res)

	return res, nil
}

// 손절 주문
func (ty *UpDownAlgorithm) StopLossOrder(Longshort string, Openclose string, Price string) (*futures.CreateOrderResponse, error) {
	res, err := ty.binance.SendOrderStopLoss(ty.Symbol, Longshort, Openclose, Price)
	if err != nil {
		log.Fatalf("Failed to send stop loss order: %v", err)
		return nil, err
	}
	fmt.Printf("Stop loss order response: %v\n", res)

	return res, nil
}
