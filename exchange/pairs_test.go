package exchange

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitAssetQuote(t *testing.T) {
	tt := []struct {
		Pair  string
		Asset string
		Quote string
	}{
		{"BTCUSDT", "BTC", "USDT"},
		{"ETHBTC", "ETH", "BTC"},
		{"BTCBUSD", "BTC", "BUSD"},
		{"1000SHIBBUSD", "1000SHIB", "BUSD"},
	}

	for _, tc := range tt {
		t.Run(tc.Pair, func(t *testing.T) {
			asset, quote := SplitAssetQuote(tc.Pair)
			require.Equal(t, tc.Asset, asset)
			require.Equal(t, tc.Quote, quote)
		})
	}
}

func TestUpdatePairFile(t *testing.T) {
	t.Skip() // it is not a test, just utility function to update pairs list
	err := updateParisFile()
	require.NoError(t, err)
}
func TestASD(t *testing.T) {
	symbol := "BTCUSDT"
	// 获取Quote和Asset
	quote := symbol[len(symbol)-4:]          // 从倒数第四个字符开始取到末尾
	asset := symbol[:len(symbol)-len(quote)] // 从开头取到倒数第四个字符前一个

	fmt.Println(asset)
	fmt.Println(quote)
}
