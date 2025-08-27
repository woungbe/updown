package mybinance

import (
	"context"

	"github.com/adshao/go-binance/v2/futures"
)

type BinanceUser struct {
	AccessKey string
	SecritKey string
	Client    *futures.Client
}

func (ty *BinanceUser) Init(apikey, seckey string) {
	ty.AccessKey = apikey
	ty.SecritKey = seckey
	ty.Client = futures.NewClient(apikey, seckey)
}

// 웹소켓 리슨키
// https://fapi.binance.com/fapi/v1/listenKey?timestamp=1716615629523&signature=bc5d84e0a8860ae3fa9d8f5581b7ecdcdb643909defcae5b36c1ecbf252d0851
// StartUserStreamService // (listenKey string, err error)
// NewStartUserStreamService
func (ty *BinanceUser) GetStartUserStreamService() (string, error) {
	res, err := ty.Client.NewStartUserStreamService().Do(context.Background())
	return res, err
}

// 레버리지 바스켓
// https://fapi.binance.com/fapi/v1/leverageBracket?timestamp=1716615629894&signature=6bcdb2b8c69a65716a7f02a7ecf51505ac15df33e97e9b1897109b37cf9163c2
func (ty *BinanceUser) GetLeverageBracket(symbol string) ([]*futures.LeverageBracket, error) {
	// symbol := "BTCUSDT"
	res, err := ty.Client.NewGetLeverageBracketService().
		Symbol(symbol).
		Do(context.Background())
	return res, err
}

// 레버리지 지정하기
func (ty *BinanceUser) SetLeverage(symbol string, leverage int) (*futures.SymbolLeverage, error) {
	res, err := ty.Client.NewChangeLeverageService().Symbol(symbol).Leverage(leverage).Do(context.Background())
	return res, err
}

// GET - positionSide  가져오기
// https://fapi.binance.com/fapi/v1/positionSide/dual?timestamp=1716615629620&signature=e3fa62aae51a1ea5de9bcff8aa25372853bb5dff336bebbb136e12cc2a4c7540
// NewChangePositionModeService (err error)
// DualSide(dualSide bool)
// positionSide 변경하기
func (ty *BinanceUser) GetChangePositionModeService(dualSide bool) error {
	err := ty.Client.NewChangePositionModeService().DualSide(dualSide).Do(context.Background())
	return err
}

// 벨런스 가져오기
// https://fapi.binance.com/fapi/v2/balance?timestamp=1716615629897&signature=4e74aacd9b88b15d7bfa245b721d422cadcbf47ff0f36d7d4380ff02cebf4fdf
// NewGetBalanceService (res []*Balance, err error)
func (ty *BinanceUser) GetBalanceService() ([]*futures.Balance, error) {
	res, err := ty.Client.NewGetBalanceService().Do(context.Background())
	return res, err
}

func (ty *BinanceUser) GetAvailableBalance(assets string) (string, error) {
	// res, err := ty.Client.NewGetBalanceService().Do(context.Background())
	// return res, err
	var AvailableBalance string
	res, err := ty.GetBalanceService()
	if err != nil {
		return "", err
	}

	for _, v := range res {
		if v.Asset == assets {
			AvailableBalance = v.AvailableBalance
		}
	}

	return AvailableBalance, err
}

// 포지션 가져오기
// https://fapi.binance.com/fapi/v2/positionRisk?timestamp=1716615629898&signature=3a78634c9e92afef0054bbe8c399f0772d55a6c4b8ad29fdc88cf17fa8edf841
// NewGetPositionRiskService (res []*PositionRisk, err error)
/*
	MarginAsset(marginAsset string)
	Pair(pair string)
*/
func (ty *BinanceUser) GetPositionRiskService(symbol string) ([]*futures.PositionRisk, error) {
	service := ty.Client.NewGetPositionRiskService()
	if symbol != "" {
		service = service.Symbol(symbol)
	}
	res, err := service.Do(context.Background())
	return res, err
}

// 오픈 오더 가져오기
// https://fapi.binance.com/fapi/v1/openOrders?timestamp=1716615629898&signature=3a78634c9e92afef0054bbe8c399f0772d55a6c4b8ad29fdc88cf17fa8edf841
// NewListOpenOrdersService (res []*Order, err error)
// Symbol(symbol string)
// Pair(pair string)
func (ty *BinanceUser) GetListOpenOrdersService(symbol string) ([]*futures.Order, error) {
	service := ty.Client.NewListOpenOrdersService()
	if symbol != "" {
		service = service.Symbol(symbol)
	}
	res, err := service.Do(context.Background())
	return res, err
}

// 주문 하기
// https://fapi.binance.com/fapi/v1/order?symbol=BIGTIMEUSDT&side=SELL&positionSide=SHORT&type=LIMIT&quantity=470&price=0.2124&timeInForce=GTC&timestamp=1716615903180&signature=05ac596440245f14ff96b584ec921933e4b66caabfd04dd225df3dc673a589bd
// NewGetOrderService (res *Order, err error)
// NewCreateOrderService
/*

Symbol(symbol string) *CreateOrderService {
Side(side SideType) *CreateOrderService {
PositionSide(positionSide PositionSideType) *CreateOrderService {
Type(orderType OrderType) *CreateOrderService {
TimeInForce(timeInForce TimeInForceType) *CreateOrderService {
Quantity(quantity string) *CreateOrderService {
ReduceOnly(reduceOnly bool) *CreateOrderService {
Price(price string) *CreateOrderService {
NewClientOrderID(newClientOrderID string) *CreateOrderService {
StopPrice(stopPrice string) *CreateOrderService {
WorkingType(workingType WorkingType) *CreateOrderService {
ActivationPrice(activationPrice string) *CreateOrderService {
CallbackRate(callbackRate string) *CreateOrderService {
PriceProtect(priceProtect bool) *CreateOrderService {
NewOrderResponseType(newOrderResponseType NewOrderRespType) *CreateOrderService {
ClosePosition(closePosition bool) *CreateOrderService {
*/

type OrderType struct {
	Symbol               string
	Side                 futures.SideType
	PositionSide         futures.PositionSideType // string
	OrderType            futures.OrderType
	TimeInForce          futures.TimeInForceType
	Quantity             string
	ReduceOnly           bool
	Price                string
	NewClientOrderID     string
	StopPrice            string
	WorkingType          futures.WorkingType
	ActivationPrice      string
	CallbackRate         string
	PriceProtect         bool
	NewOrderResponseType futures.NewOrderRespType
	ClosePosition        bool
}

type OpenOrder struct {
	Symbol        string                   // BIGTIMEUSDT
	Side          futures.SideType         // string // SELL
	PositionSide  futures.PositionSideType // string           // SHORT
	Type          futures.OrderType        // string                   // LIMIT, MARKET
	Quantity      string                   // 464
	Price         string                   // 0.2151
	TimeInForce   futures.TimeInForceType  // string                   // GTC
	StopPrice     string
	WorkingType   futures.WorkingType
	ClosePosition string
}

func (ty *BinanceUser) CreateOrderLimitMarket(args OpenOrder) *futures.CreateOrderService {
	order := new(futures.CreateOrderService)
	// 주문 목록 업데이트 좀 해야지 ?!!
	if args.Symbol != "" {
		order = order.Symbol(args.Symbol)
	}

	if args.Side != "" {
		order = order.Side(args.Side)
	}

	if args.PositionSide != "" {
		order = order.PositionSide(args.PositionSide)
	}

	if args.Type != "" {
		order = order.Type(args.Type)
	}

	if args.Quantity != "" {
		order = order.Quantity(args.Quantity)
	}

	if args.Price != "" {
		order = order.Price(args.Price)
	}

	if args.TimeInForce != "" {
		order = order.TimeInForce(args.TimeInForce)
	}

	if args.WorkingType != "" {
		order = order.WorkingType(args.WorkingType)
	}

	if args.ClosePosition != "" {
		var v bool
		v = false
		if args.ClosePosition == "true" {
			v = true
		}
		order = order.ClosePosition(v)
	}

	return order
}

// res *futures.CreateOrderResponse
func (ty *BinanceUser) CreateOrderService(aa OrderType) (*futures.CreateOrderResponse, error) {
	service := ty.Client.NewCreateOrderService()

	if aa.Symbol != "" {
		service.Symbol(aa.Symbol)
	}

	if aa.Side != "" {
		service.Side(aa.Side)
	}

	if aa.PositionSide != "" {
		service.PositionSide(aa.PositionSide)
	}

	if aa.OrderType != "" {
		service.Type(aa.OrderType)
	}

	if aa.TimeInForce != "" {
		service.TimeInForce(aa.TimeInForce)
	}

	if aa.Quantity != "" {
		service.Quantity(aa.Quantity)
	}

	if aa.Price != "" {
		service.Price(aa.Price)
	}

	if aa.StopPrice != "" {
		service.StopPrice(aa.StopPrice)
	}

	if aa.WorkingType != "" {
		service.WorkingType(aa.WorkingType)
	}

	if aa.ClosePosition {
		service.ClosePosition(aa.ClosePosition)
	}

	res, err := service.Do(context.Background())
	return res, err
}

func (ty *BinanceUser) CreateMuiOrder(orders []*futures.CreateOrderService) (*futures.CreateBatchOrdersResponse, error) {
	res, err := ty.Client.NewCreateBatchOrdersService().OrderList(orders).Do(context.Background())
	return res, err
}

// 미체결 주문 삭제
func (ty *BinanceUser) CancelOrder(symbol, orgiclientOrderID string) (*futures.CancelOrderResponse, error) {
	// bin := futures.NewClient(ty.AccessKey, ty.SecritKey)
	res, err := ty.Client.NewCancelOrderService().Symbol(symbol).OrigClientOrderID(orgiclientOrderID).Do(context.Background())
	return res, err
}

// 유저 스트림 갱신
func (ty *BinanceUser) KeepAliveUserStream(listenKey string) error {
	err := ty.Client.NewKeepaliveUserStreamService().ListenKey(listenKey).Do(context.Background())
	return err
}
