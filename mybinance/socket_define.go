package mybinance

type OrderTradeUpdate struct {
	EventType string `json:"e"`
	Time      int64  `json:"T"`
	EventTime int64  `json:"E"`
	Order     struct {
		Symbol                   string `json:"s"`   // 주문 심볼
		ClientOrderID            string `json:"c"`   // 클라이언트 주문 ID
		Side                     string `json:"S"`   // BUY, SELL
		OrderType                string `json:"o"`   // LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER
		TimeInForce              string `json:"f"`   // GTC, IOC, FOK
		Quantity                 string `json:"q"`   // 주문 수량
		Price                    string `json:"p"`   // 주문 가격
		AveragePrice             string `json:"ap"`  // 평균 체결 가격
		StopPrice                string `json:"sp"`  // 중지 가격
		CurrentExecutionType     string `json:"x"`   // NEW, PARTIALLY_FILLED, FILLED, CANCELED, PENDING_CANCEL, REJECTED, EXPIRED
		CurrentOrderStatus       string `json:"X"`   // NEW, PARTIALLY_FILLED, FILLED, CANCELED, PENDING_CANCEL, REJECTED, EXPIRED
		OrderID                  int64  `json:"i"`   // 주문 ID
		LastFilledQuantity       string `json:"l"`   // 채결된 체결량
		CumulativeFilledQuantity string `json:"z"`   // 누적 체결량
		LastFilledPrice          string `json:"L"`   // 체결 가격
		CommissionAmount         string `json:"n"`   // 수수료
		CommissionAsset          string `json:"N"`   // 수수료 자산
		TransactionTime          int64  `json:"T"`   // 체결 시간
		TradeID                  int64  `json:"t"`   // 체결 ID
		BidsNotional             string `json:"b"`   // 매수 종가 가치
		AsksNotional             string `json:"a"`   // 매도 종가 가치
		IsMakerSide              bool   `json:"m"`   // true, false
		IsReduceOnly             bool   `json:"R"`   // true, false
		WorkingType              string `json:"wt"`  // MARK_PRICE, CONTRACT_PRICE
		OriginalOrderType        string `json:"ot"`  // LIMIT, LIMIT_MAKER
		PositionSide             string `json:"ps"`  // BOTH, LONG, SHORT
		IsClosePosition          bool   `json:"cp"`  // true, false
		ActivationPrice          string `json:"rp"`  // 활성화 가격
		CallbackRate             string `json:"pP"`  // 콜백 비율
		StopIsolated             int    `json:"si"`  // 중지 단일 중지 주문
		StopShared               int    `json:"ss"`  // 중지 공유 중지 주문
		OrderVisibility          string `json:"V"`   // true, false
		PriceMatch               string `json:"pm"`  // true, false
		GoodTillDate             int64  `json:"gtd"` // 유효 기간
	} `json:"o"`
}

type AccountUpdate struct {
	EventType string `json:"e"` // ACCOUNT_UPDATE
	Time      int64  `json:"T"`
	EventTime int64  `json:"E"`
	Account   struct {
		Balances []struct {
			Asset              string `json:"a"`
			WalletBalance      string `json:"wb"`
			CrossWalletBalance string `json:"cw"`
			BalanceChange      string `json:"bc"`
		} `json:"B"`
		Positions []struct {
			Symbol       string `json:"s"`
			EntryPrice   string `json:"ep"`
			Amount       string `json:"am"`
			PositionSide string `json:"ps"`
			UpdateTime   int64  `json:"up"`
		} `json:"P"`
		Reason string `json:"m"`
	} `json:"a"`
}
