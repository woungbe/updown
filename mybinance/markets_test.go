package mybinance

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestMarkets(t *testing.T) {
	// (*futures.ExchangeInfo, error)
	res, err := GetExchangeInfo()
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range res.Symbols {
		fmt.Printf("PriceFilter %+v\n", JsonData(v.PriceFilter()))
		fmt.Printf("PercentPriceFilter %+v\n", JsonData(v.PercentPriceFilter()))
		fmt.Printf("MarketLotSizeFilter %+v\n", JsonData(v.MarketLotSizeFilter()))
		fmt.Printf("MaxNumOrdersFilter %+v\n", JsonData(v.MaxNumOrdersFilter()))
		fmt.Printf("MaxNumAlgoOrdersFilter %+v\n", JsonData(v.MaxNumAlgoOrdersFilter()))
		fmt.Printf("MIN_NOTIONAL %+v\n", JsonData(v.MinNotionalFilter()))

		// minnotmal := v.MinNotionalFilter()
		// fmt.Println("minnotmal.Notional : ", minnotmal.Notional)

		fmt.Printf("LotSizeFilter %+v\n", JsonData(v.LotSizeFilter()))
		lotsize := v.LotSizeFilter()
		fmt.Println("lotsize.MinQuantity : ", lotsize.MinQuantity)

	}
}

func TestTicker(t *testing.T) {
	ticker, err := GetTicker("BTCUSDT")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(JsonData(ticker))
}

func TestGetPremiumIndex(t *testing.T) {
	res, err := GetPremiumIndex()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(JsonData(res))
}

func TestGetNewServerTime(t *testing.T) {
	res, err := GetNewServerTime()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(JsonData(res))
}
func JsonData(aa interface{}) string {
	jsonData, err := json.MarshalIndent(aa, "", "    ")
	if err != nil {
		fmt.Printf("Error encoding JSON: %s\n", err.Error())
		return ""
	}

	return string(jsonData)
}
