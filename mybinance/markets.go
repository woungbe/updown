package mybinance

import (
	"context"

	"github.com/adshao/go-binance/v2/futures"
)

// 마컷 정보 가져오기
// https://fapi.binance.com/fapi/v1/exchangeInfo
func GetExchangeInfo() (*futures.ExchangeInfo, error) {
	// binance.NewExchangeInfoService()
	bin := futures.NewClient("", "")
	res, err := bin.NewExchangeInfoService().Do(context.Background())
	return res, err

	// res.Symbols = []Symbol
	/*
		LotSizeFilter() *LotSizeFilter
		PriceFilter() *PriceFilter
		PercentPriceFilter() *PercentPriceFilter
		MarketLotSizeFilter() *MarketLotSizeFilter
		MaxNumOrdersFilter() *MaxNumOrdersFilter
		MaxNumAlgoOrdersFilter() *MaxNumAlgoOrdersFilter
	*/
}

// 티커 데이터 가져오기
// https://fapi.binance.com/fapi/v1/ticker/24hr

func GetTicker(symbol string) ([]*futures.PriceChangeStats, error) {
	// symbol := "BTCUSDT"
	bin := futures.NewClient("", "")
	res, err := bin.NewListPriceChangeStatsService().
		Symbol(symbol).
		Do(context.Background())
	// res []*PriceChangeStats, err error
	return res, err
}

// preminumIndex
//
//	https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BIGTIMEUSDT
func GetPremiumIndex() ([]*futures.PremiumIndex, error) {
	bin := futures.NewClient("", "")
	res, err := bin.NewPremiumIndexService().Do(context.Background())
	return res, err
}

// 타임 가져오기
// https://fapi.binance.com/fapi/v1/time
func GetNewServerTime() (int64, error) {
	bin := futures.NewClient("", "")
	res, err := bin.NewServerTimeService().Do(context.Background())
	return res, err
}
