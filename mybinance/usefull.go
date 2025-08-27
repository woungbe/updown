package mybinance

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/spf13/viper"
	"github.com/woungbe/utils"
)

func GetRelativePathToEnv() string {
	// 현재 실행 중인 디렉토리를 얻음 (프로그램이 실행되는 위치)
	execDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Error getting working directory: %v", err)
	}

	// GetKey 함수가 정의된 파일의 절대 경로를 얻음
	_, filename, _, ok := runtime.Caller(0) // 0은 현재 함수 (GetKey)를 의미
	if !ok {
		log.Fatalf("Error getting the current file path")
	}

	// 현재 파일이 위치한 디렉토리 경로를 추출
	fileDir := filepath.Dir(filename)

	// 두 경로 간의 상대 경로를 계산
	relPath, err := filepath.Rel(execDir, fileDir)
	if err != nil {
		log.Fatalf("Error calculating relative path: %v", err)
	}

	// .env 파일을 향한 상대 경로 생성
	// envPath := filepath.Join(relPath, "../.env")
	envPath := filepath.Join(relPath, "../.dev.env")

	return envPath
}

func GetKey() (string, string) {
	envPath := GetRelativePathToEnv()
	viper.SetConfigFile(envPath)
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	// viper.SetConfigFile(path)
	viper.AutomaticEnv() // Automatically use environment variables as well

	// Get the keys from the .env file
	apiKey := viper.GetString("AccessKey")
	secKey := viper.GetString("SecretKey")

	return apiKey, secKey
}

// 미체결 주문 하기
func (ty *BinanceUser) SendOpenOrder(symbol, position, openclose, price, amount string) (*futures.CreateBatchOrdersResponse, error) {
	var send []*futures.CreateOrderService
	var order OpenOrder
	order.Symbol = symbol
	order.PositionSide = PositionSide(position)         // PositionSideTypeLong PositionSideTypeShort
	order.Side = SideType(GetSide(position, openclose)) // SideTypeBuy SideTypeSell
	order.Type = futures.OrderTypeLimit                 // OrderTypeLimit OrderTypeMarket
	order.Price = price
	order.Quantity = amount
	order.TimeInForce = futures.TimeInForceTypeGTC
	createOrderService := ty.CreateOrderLimitMarket(order)
	send = append(send, createOrderService)
	res, err := ty.CreateMuiOrder(send)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)

	if err != nil {
		return nil, err
	}
	return res, err
}

// 마켓 주문
func (ty *BinanceUser) SendOrderMarket(symbol, position, openclose, amount string) (*futures.CreateOrderResponse, error) {
	var order OrderType
	order.Symbol = symbol
	order.PositionSide = PositionSide(position)         // PositionSideTypeLong PositionSideTypeShort
	order.Side = SideType(GetSide(position, openclose)) // SideTypeBuy SideTypeSell
	order.OrderType = futures.OrderTypeMarket           // OrderTypeLimit OrderTypeMarket
	order.Quantity = amount
	// order.TimeInForce = futures.TimeInForceTypeGTC
	res, err := ty.CreateOrderService(order)
	if err != nil {
		return nil, err
	}
	return res, err
}

// take profit - 익절 주문
func (ty *BinanceUser) SendOrderTakeProfit(symbol, position, openclose, price string) (*futures.CreateOrderResponse, error) {
	var order OrderType
	order.Symbol = symbol
	order.PositionSide = PositionSide(position)         // PositionSideTypeLong PositionSideTypeShort
	order.Side = SideType(GetSide(position, openclose)) // SideTypeBuy SideTypeSell
	order.OrderType = futures.OrderTypeTakeProfitMarket // OrderTypeLimit OrderTypeMarket OrderTypeTakeProfitMarket
	order.StopPrice = price
	order.ClosePosition = true
	order.WorkingType = futures.WorkingTypeContractPrice // WorkingTypeContractPrice
	fmt.Printf("order : %+v", order)
	res, err := ty.CreateOrderService(order)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// StopLoss - 모든 수량을 팔아서 포지션을 정리함. - 손절 주문
func (ty *BinanceUser) SendOrderStopLoss(symbol, position, openclose, price string) (*futures.CreateOrderResponse, error) {
	/*
		symbol: BIGTIMEUSDT
		side: SELL
		positionSide: LONG
		type: STOP_MARKET
		stopPrice: 0.2051
		closePosition: true
	*/
	var order OrderType
	order.Symbol = symbol
	order.PositionSide = PositionSide(position)         // PositionSideTypeLong PositionSideTypeShort
	order.Side = SideType(GetSide(position, openclose)) // SideTypeBuy SideTypeSell
	order.OrderType = futures.OrderTypeStopMarket       // OrderTypeLimit OrderTypeMarket OrderTypeTakeProfitMarket
	order.StopPrice = price
	order.ClosePosition = true
	order.WorkingType = futures.WorkingTypeContractPrice // WorkingTypeContractPrice
	res, err := ty.CreateOrderService(order)
	return res, err

}

// stopmarket 은 포지션을 종료하지 않음 - 수량대로 더 사거나, 더 팔 수 있음
// BITTIMEUSDT, SHORT, OPEN, "0.210", "500"
func (ty *BinanceUser) SendOrderStopMarket(symbol, position, openclose, price, amount string) (*futures.CreateOrderResponse, error) {
	var order OrderType
	order.Symbol = symbol
	order.PositionSide = PositionSide(position)         // PositionSideTypeLong PositionSideTypeShort
	order.Side = SideType(GetSide(position, openclose)) // SideTypeBuy SideTypeSell
	order.OrderType = futures.OrderTypeStopMarket       // OrderTypeLimit OrderTypeMarket OrderTypeTakeProfitMarket
	order.StopPrice = price
	order.Quantity = amount
	order.ClosePosition = false
	order.WorkingType = futures.WorkingTypeContractPrice // WorkingTypeContractPrice
	res, err := ty.CreateOrderService(order)
	return res, err
}

// 트레일링 스탑
func (ty *BinanceUser) SendOrderTrailingStop(symbol, position, openclose, price, amount string) (*futures.CreateOrderResponse, error) {
	/*
		symbol: BIGTIMEUSDT
		side: SELL
		positionSide: LONG
		type: TRAILING_STOP_MARKET
		quantity: 500
		callbackRate: 1
		workingType: CONTRACT_PRICE
		activationPrice: 0.2104
	*/
	var order OrderType
	order.Symbol = symbol
	order.PositionSide = PositionSide(position)         // PositionSideTypeLong PositionSideTypeShort
	order.Side = SideType(GetSide(position, openclose)) // SideTypeBuy SideTypeSell
	order.OrderType = futures.OrderTypeStopMarket       // OrderTypeLimit OrderTypeMarket OrderTypeTakeProfitMarket
	order.ActivationPrice = price
	order.Quantity = amount
	order.WorkingType = futures.WorkingTypeContractPrice // WorkingTypeContractPrice
	res, err := ty.CreateOrderService(order)
	return res, err
}

// 해당 심볼의 미체결을 모두 취소하기
func (ty *BinanceUser) SendRemoveOpenOrderForSymbol(symbol string) []string {
	// ([]*futures.Order, error)
	res, err := ty.GetListOpenOrdersService(symbol)
	if err != nil {
		fmt.Println(err)
	}

	var orderID []string
	for _, v := range res {
		val := utils.String(v.ClientOrderID)
		orderID = append(orderID, val)
	}

	msg := ty.SendRemoveOpenOrder(symbol, orderID)
	if len(msg) != 0 {
		return msg
	}
	var send []string
	return send
}

// 해당 미체결 모두 정리
func (ty *BinanceUser) SendRemoveOpenOrder(symbol string, orderID []string) []string {
	var send []string
	for _, v := range orderID {
		_, err := ty.CancelOrder(symbol, v)
		if err != nil {
			send = append(send, fmt.Sprintf("%s %s", v, err))
		}
	}
	return send
}

func SideType(sidetype string) futures.SideType {
	// Side = futures.SideTypeBuy                  // SideTypeBuy SideTypeSell
	if sidetype == "BUY" {
		return futures.SideTypeBuy
	}

	if sidetype == "SELL" {
		return futures.SideTypeSell
	}
	return ""
}

func PositionSide(positionside string) futures.PositionSideType {
	// PositionSide = futures.futures.PositionSideTypeLong // PositionSideTypeLong PositionSideTypeShort
	if positionside == "LONG" {
		return futures.PositionSideTypeLong
	}

	if positionside == "SHORT" {
		return futures.PositionSideTypeShort
	}

	return ""
}

// side 가져오기
func GetSide(posside, openClose string) string {

	tmpposside := strings.ToUpper(posside)
	tmpopenClose := strings.ToUpper(openClose)

	if tmpposside == "LONG" && tmpopenClose == "OPEN" {
		return "BUY"
	}
	if tmpposside == "LONG" && tmpopenClose == "CLOSE" {
		return "SELL"
	}
	if tmpposside == "SHORT" && tmpopenClose == "OPEN" {
		return "SELL"
	}
	if tmpposside == "SHORT" && tmpopenClose == "CLOSE" {
		return "BUY"
	}

	return ""

}
