package main

import (
	"log"
	"os"
	"time"

	"woungbe/updown_0.1/test/binanace/socket/price-concel/pricecache"

	"github.com/joho/godotenv"
)

func main(){
	err := godotenv.Load()
	if err != nil {
		log.Println(".env 파일을 찾을 수 없거나 로드 실패:", err)
	}

	// symbols := []string{"BTCUSDT"}
	ps := pricecache.FuturePriceStatus{}
	apiKey := os.Getenv("BINANCE_API_KEY")
	ps.Init(apiKey)

	ps.AddSymbol("BTCUSDT")

	go func() {
		for {
			price, ok := ps.GetPrice("BTCUSDT", "LONG")	
			if ok {
				log.Printf("price: %f", price)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	select {}
}
