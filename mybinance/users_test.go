package mybinance

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/spf13/viper"
)

func GetEnv() (string, string) {
	viper.SetConfigFile("../.dev.env") // .env 파일 설정
	viper.AutomaticEnv()               // 환경 변수를 자동으로 읽도록 설정
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	accKey := viper.GetString("AccessKey")
	seckey := viper.GetString("SecretKey")
	return accKey, seckey
}

func GetUsrs() *BinanceUser {
	AccessKey, SecritKey := GetEnv()
	binance := new(BinanceUser)
	binance.Init(AccessKey, SecritKey)
	return binance
}

func GetUsrsData(AccessKey, SecritKey string) *BinanceUser {
	binance := new(BinanceUser)
	binance.Init(AccessKey, SecritKey)
	return binance
}

func TestGetStartUserStreamService(t *testing.T) {
	res, err := GetUsrs().GetStartUserStreamService()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

func TestGetLeverageBracket(t *testing.T) {
	res, err := GetUsrs().GetLeverageBracket("REEFUSDT")
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range res {
		fmt.Println(v.Symbol)
		for _, k := range v.Brackets {
			fmt.Printf("%d %d %.0f\n", k.InitialLeverage, k.Bracket, float64(k.NotionalCap))
		}
	}
}

// 레버리지 변경하기
func TestSetLeverage(t *testing.T) {
	res, err := GetUsrs().SetLeverage("REEFUSDT", 10)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

// positionMode
func TestGetChangePositionModeService(t *testing.T) {
	err := GetUsrs().GetChangePositionModeService(true)
	if err != nil {
		fmt.Println(err)
	}
}

func TestGetBalanceService(t *testing.T) {
	res, err := GetUsrs().GetBalanceService()
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range res {
		// fmt.Println(v.AccountAlias, v.Asset, v.Balance, v.CrossWalletBalance, v.CrossUnPnl, v.AvailableBalance, v.MaxWithdrawAmount)
		if v.Asset == "USDT" {
			fmt.Println("v.AvailableBalance ", v.AvailableBalance)
		}
	}
}

func TestGetPositionRiskService(t *testing.T) {
	res, err := GetUsrs().GetPositionRiskService("REEFUSDT")
	if err != nil {
		fmt.Println(err)
	}
	// 무조건 2개씩 나옴 LONG,SHORT
	for _, v := range res {
		fmt.Println(v.Symbol, v.PositionAmt, v.EntryPrice, v.Leverage, v.UnRealizedProfit, v.PositionSide)
	}
}

func TestGetListOpenOrdersService(t *testing.T) {
	res, err := GetUsrs().GetListOpenOrdersService("")
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range res {
		fmt.Printf("%+v\n", v)
	}
}

func TestCreateMuiOrder(t *testing.T) {
	var send []*futures.CreateOrderService
	var order OpenOrder
	order.Symbol = "REEFUSDT"
	order.Side = futures.SideTypeBuy                  // SideTypeBuy SideTypeSell
	order.PositionSide = futures.PositionSideTypeLong // PositionSideTypeLong PositionSideTypeShort
	order.Type = futures.OrderTypeLimit               // OrderTypeLimit OrderTypeMarket
	order.Quantity = "8988"
	order.Price = "0.001400"
	order.TimeInForce = futures.TimeInForceTypeGTC

	createOrderService := GetUsrs().CreateOrderLimitMarket(order)
	send = append(send, createOrderService)
	res, err := GetUsrs().CreateMuiOrder(send)
	if err != nil {
		fmt.Println(err)
	}
	jsonOutput, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}
	fmt.Println(string(jsonOutput))
}

func TestCreateMuiOrderMarket(t *testing.T) {
	var send []*futures.CreateOrderService
	var order OpenOrder
	order.Symbol = "BIGTIMEUSDT"
	order.Side = futures.SideTypeSell                  // SideTypeBuy SideTypeSell
	order.PositionSide = futures.PositionSideTypeShort // PositionSideTypeLong PositionSideTypeShort
	order.Type = futures.OrderTypeMarket               // OrderTypeLimit OrderTypeMarket
	order.Quantity = "500"
	// order.Price = "0.2177"
	// order.TimeInForce = futures.TimeInForceTypeGTC

	createOrderService := GetUsrs().CreateOrderLimitMarket(order)
	send = append(send, createOrderService)
	res, err := GetUsrs().CreateMuiOrder(send)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

// 시장가 청산
func TestCreateClosePosition(t *testing.T) {
	var send []*futures.CreateOrderService
	var order OpenOrder
	order.Symbol = "BIGTIMEUSDT"
	order.Side = futures.SideTypeBuy                   // SideTypeBuy SideTypeSell
	order.PositionSide = futures.PositionSideTypeShort // PositionSideTypeLong PositionSideTypeShort
	order.Type = futures.OrderTypeMarket               // OrderTypeLimit OrderTypeMarket
	order.Quantity = "500"

	createOrderService := GetUsrs().CreateOrderLimitMarket(order)
	send = append(send, createOrderService)
	res, err := GetUsrs().CreateMuiOrder(send)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

func TestCreateMuiOrderTakeProfit(t *testing.T) {
	// var send []*futures.CreateOrderService
	var order OrderType
	order.Symbol = "BIGTIMEUSDT"
	order.Side = futures.SideTypeBuy                    // SideTypeBuy SideTypeSell
	order.PositionSide = futures.PositionSideTypeShort  // PositionSideTypeLong PositionSideTypeShort
	order.OrderType = futures.OrderTypeTakeProfitMarket // OrderTypeLimit OrderTypeMarket OrderTypeTakeProfitMarket
	order.StopPrice = "0.210"
	order.ClosePosition = true
	order.WorkingType = futures.WorkingTypeContractPrice // WorkingTypeContractPrice
	// createOrderService := CreateOrderLimitMarket(order)
	// send = append(send, createOrderService)
	// res, err := GetUsrs().CreateMuiOrder(send)
	res, err := GetUsrs().CreateOrderService(order)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

func TestCreateMuiOrderStopMarket(t *testing.T) {
	// var send []*futures.CreateOrderService
	var order OrderType
	order.Symbol = "BIGTIMEUSDT"
	order.Side = futures.SideTypeBuy                  // SideTypeBuy SideTypeSell
	order.PositionSide = futures.PositionSideTypeLong // PositionSideTypeLong PositionSideTypeShort
	order.OrderType = futures.OrderTypeStopMarket     // OrderTypeLimit OrderTypeMarket OrderTypeTakeProfitMarket
	order.StopPrice = "0.2150"
	order.Quantity = "500"
	order.ClosePosition = false
	order.WorkingType = futures.WorkingTypeContractPrice // WorkingTypeContractPrice
	// createOrderService := CreateOrderLimitMarket(order)
	// send = append(send, createOrderService)
	// res, err := GetUsrs().CreateMuiOrder(send)
	res, err := GetUsrs().CreateOrderService(order)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}
