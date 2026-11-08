package exchange

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bengalm/ninjabot/model"
	"github.com/bengalm/ninjabot/service"
	"github.com/bengalm/ninjabot/tools/log"
)

// OKX USDT-margined SWAP adapter for ninjabot.
//
// Implements the same surface as BinanceFuture (service.Exchange).
// Notes:
//   - Quantities are exposed to ninjabot in COINS (base asset units), internally
//     converted to OKX contract counts (sz = notional / (ctVal * price)).
//   - Stop / Take-Profit / Trailing stops are OKX algo orders
//     (POST /api/v5/trade/order-algo).
//   - Candle feed is REST polling (newly CLOSED candles only), staggered across pairs.
//
// API quirks honored (verified on this host):
//   - base URL must be https://www.okx.com (api.okx.com returns 530)
//   - a browser User-Agent header is required, otherwise 403 / error 1010
//   - OK-ACCESS-SIGN = base64(hmac-sha256(ts+METHOD+path+body, secret))
//   - OK-ACCESS-TIMESTAMP must be ISO8601, e.g. 2026-11-07T08:00:00.000Z

const (
	okxBaseURL = "https://www.okx.com"
	okxUA      = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

// OKXPairOption configures one instrument (leverage is always isolated margin).
type OKXPairOption struct {
	Pair     string // OKX style, e.g. "BTC-USDT-SWAP"
	Leverage int
}

// okxOrderRef remembers the real OKX order/algo id behind the int64 ExchangeID
// used by ninjabot's model.Order.
type okxOrderRef struct {
	OKXID string
	Algo  bool
}

type OKX struct {
	ctx    context.Context
	client *http.Client

	APIKey     string
	SecretKey  string
	Passphrase string
	Testnet    bool

	PairOptions []OKXPairOption

	pairs       []string // successfully configured pairs (OKX style)
	assetsInfo  map[string]model.AssetInfo
	ctVal       map[string]float64 // pair -> contract value in base coins
	lotSz       map[string]float64 // pair -> lot size in contracts
	minSz       map[string]float64 // pair -> min size in contracts
	tickSz      map[string]float64
	tickPrec    map[string]int
	lotPrec     map[string]int
	ordIDs      sync.Map // int64 -> okxOrderRef
	serverSkew  time.Duration
	rateMu      sync.Mutex
	lastReq     time.Time
	minInterval time.Duration // 全局最小请求间隔(默认120ms)
	accountMu   sync.Mutex
	accountData *model.Account
	accountTime time.Time
	subCounter  atomic.Int64
}

// Ensure interface compliance at compile time.
var _ service.Exchange = (*OKX)(nil)

type OKXOption func(*OKX)

// WithOKXCredentials sets API credentials for OKX.
func WithOKXCredentials(key, secret, passphrase string) OKXOption {
	return func(o *OKX) {
		o.APIKey = key
		o.SecretKey = secret
		o.Passphrase = passphrase
	}
}

// WithOKXTestnet routes requests to OKX demo trading (x-simulated-trading: 1).
func WithOKXTestnet() OKXOption {
	return func(o *OKX) { o.Testnet = true }
}

// WithOKXLeverage configures a pair with the given leverage (isolated margin).
func WithOKXLeverage(pair string, leverage int) OKXOption {
	return func(o *OKX) {
		o.PairOptions = append(o.PairOptions, OKXPairOption{
			Pair:     ToOKXSymbol(pair),
			Leverage: leverage,
		})
	}
}

// ToOKXSymbol converts a plain symbol ("BTCUSDT") into an OKX SWAP instId
// ("BTC-USDT-SWAP"). Already-converted symbols pass through unchanged.
func ToOKXSymbol(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if strings.HasSuffix(s, "-SWAP") || strings.HasSuffix(s, "-FUTURES") {
		return s
	}
	s = strings.TrimSuffix(s, "USDT")
	if s == "" {
		return symbol
	}
	return s + "-USDT-SWAP"
}

// OKXSplitAssetQuote splits "BTC-USDT-SWAP" into ("BTC", "USDT").
func OKXSplitAssetQuote(pair string) (string, string) {
	s := strings.TrimSuffix(pair, "-SWAP")
	parts := strings.Split(s, "-")
	if len(parts) < 2 {
		return s, "USDT"
	}
	return parts[len(parts)-2], parts[len(parts)-1]
}

// NewOKX creates an OKX adapter: fetches SWAP instruments, builds the asset
// info for the requested pairs (logging dropped/unavailable ones) and sets
// leverage to the configured value with isolated margin.
func NewOKX(ctx context.Context, options ...OKXOption) (*OKX, error) {
	o := &OKX{
		ctx:         ctx,
		client:      &http.Client{Timeout: 10 * time.Second},
		assetsInfo:  make(map[string]model.AssetInfo),
		ctVal:       make(map[string]float64),
		lotSz:       make(map[string]float64),
		minSz:       make(map[string]float64),
		tickSz:      make(map[string]float64),
		tickPrec:    make(map[string]int),
		lotPrec:     make(map[string]int),
		minInterval: 120 * time.Millisecond,
	}
	for _, opt := range options {
		opt(o)
	}

	// Calibrate clock against OKX server (best effort).
	if ts, err := o.serverTime(ctx); err == nil {
		o.serverSkew = time.Since(ts)
	}

	instruments, err := o.swapInstruments(ctx)
	if err != nil {
		return nil, fmt.Errorf("okx instruments fetch fail: %w", err)
	}
	byInstID := make(map[string]okxInstrument, len(instruments))
	for _, inst := range instruments {
		byInstID[inst.InstID] = inst
	}

	for _, po := range o.PairOptions {
		inst, ok := byInstID[po.Pair]
		if !ok {
			log.Warnf("[OKX] dropping pair %s: not found in SWAP instruments", po.Pair)
			continue
		}

		ctVal, _ := strconv.ParseFloat(inst.CtVal, 64)
		if ctVal <= 0 {
			log.Warnf("[OKX] dropping pair %s: invalid ctVal %q", po.Pair, inst.CtVal)
			continue
		}
		lotSz, _ := strconv.ParseFloat(inst.LotSz, 64)
		minSz, _ := strconv.ParseFloat(inst.MinSz, 64)
		tickSz, _ := strconv.ParseFloat(inst.TickSz, 64)
		if lotSz <= 0 {
			lotSz = 1
		}

		asset, quote := OKXSplitAssetQuote(po.Pair)
		o.assetsInfo[po.Pair] = model.AssetInfo{
			BaseAsset:          asset,
			QuoteAsset:         quote,
			MinPrice:           tickSz,
			MaxPrice:           math.MaxFloat64,
			MinQuantity:        minSz * ctVal, // in coins
			MaxQuantity:        math.MaxFloat64,
			StepSize:           lotSz * ctVal, // in coins
			TickSize:           tickSz,
			QuotePrecision:     8,
			PricePrecision:     decimals(inst.TickSz),
			BaseAssetPrecision: decimals(inst.LotSz),
		}
		o.ctVal[po.Pair] = ctVal
		o.lotSz[po.Pair] = lotSz
		o.minSz[po.Pair] = minSz
		o.tickSz[po.Pair] = tickSz
		o.tickPrec[po.Pair] = decimals(inst.TickSz)
		o.lotPrec[po.Pair] = decimals(inst.LotSz)

		// set leverage, isolated margin (限流退避: 50011重试, 币间pacing 150ms)
		if o.APIKey != "" {
			body := map[string]interface{}{
				"instId":  po.Pair,
				"lever":   strconv.Itoa(po.Leverage),
				"mgnMode": "isolated",
			}
			var levErr error
			for attempt := 0; attempt < 5; attempt++ {
				if attempt > 0 {
					time.Sleep(time.Duration(attempt) * time.Second)
				}
				_, levErr = o.request(ctx, http.MethodPost, "/api/v5/account/set-leverage", body, true)
				if levErr == nil {
					break
				}
			}
			if levErr != nil {
				log.Errorf("[OKX] set leverage %dx for %s failed after retries (继续, 首次交易前需自查): %v",
					po.Leverage, po.Pair, levErr)
			}
			time.Sleep(150 * time.Millisecond)
		}
		o.pairs = append(o.pairs, po.Pair)
		log.Infof("[OKX] pair %s ready: ctVal=%s lotSz=%s minSz=%s tickSz=%s lever=%dx isolated",
			po.Pair, inst.CtVal, inst.LotSz, inst.MinSz, inst.TickSz, po.Leverage)
	}

	log.Infof("[SETUP] Using OKX USDT SWAP exchange (%d pairs configured)", len(o.pairs))
	return o, nil
}

// Pairs returns the pairs that were successfully configured on the exchange.
func (o *OKX) Pairs() []string {
	out := make([]string, len(o.pairs))
	copy(out, o.pairs)
	return out
}

// ---------------------------------------------------------------- transport

type okxResponse struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func (o *OKX) sign(timestamp, method, path, body string) string {
	preHash := timestamp + method + path + body
	mac := hmac.New(sha256.New, []byte(o.SecretKey))
	mac.Write([]byte(preHash))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (o *OKX) isoTimestamp() string {
	return time.Now().UTC().Add(-o.serverSkew).Format("2006-01-02T15:04:05.000Z")
}

func (o *OKX) request(ctx context.Context, method, path string, body interface{}, auth bool) (json.RawMessage, error) {
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
	}

	// 全局限速: 请求间隔≥120ms(防50011) + 50011自动退避重试×4
	o.rateMu.Lock()
	wait := o.minInterval - time.Since(o.lastReq)
	if wait > 0 {
		time.Sleep(wait)
	}
	o.lastReq = time.Now()
	o.rateMu.Unlock()

	var data json.RawMessage
	var err error
	for attempt := 0; ; attempt++ {
		data, err = o.doRequest(ctx, method, path, bodyBytes, auth)
		if err == nil {
			return data, nil
		}
		if attempt >= 4 || !strings.Contains(err.Error(), "50011") {
			return nil, err
		}
		backoff := time.Duration(500*(attempt+1)) * time.Millisecond
		log.Errorf("[OKX] 50011 rate-limited %s %s, retry %d in %v", method, path, attempt+1, backoff)
		time.Sleep(backoff)
	}
}

func (o *OKX) doRequest(ctx context.Context, method, path string, bodyBytes []byte, auth bool) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, method, okxBaseURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", okxUA) // required: without it OKX answers 403 / code 1010
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	if auth {
		ts := o.isoTimestamp()
		req.Header.Set("OK-ACCESS-KEY", o.APIKey)
		req.Header.Set("OK-ACCESS-SIGN", o.sign(ts, method, path, string(bodyBytes)))
		req.Header.Set("OK-ACCESS-TIMESTAMP", ts)
		req.Header.Set("OK-ACCESS-PASSPHRASE", o.Passphrase)
	}
	if o.Testnet {
		req.Header.Set("x-simulated-trading", "1")
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var envelope okxResponse
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("okx parse response (http %d): %s", resp.StatusCode, truncate(data, 300))
	}
	if envelope.Code != "0" {
		return nil, fmt.Errorf("okx error: code=%s msg=%s %s", envelope.Code, envelope.Msg, truncate(envelope.Data, 300))
	}
	return envelope.Data, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

func decimals(s string) int {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return len(s) - i - 1
	}
	return 0
}

// ---------------------------------------------------------------- public data

type okxInstrument struct {
	InstID    string `json:"instId"`
	InstType  string `json:"instType"`
	CtVal     string `json:"ctVal"`
	CtValCcy  string `json:"ctValCcy"`
	LotSz     string `json:"lotSz"`
	MinSz     string `json:"minSz"`
	TickSz    string `json:"tickSz"`
	State     string `json:"state"`
	SettleCcy string `json:"settleCcy"`
}

func (o *OKX) serverTime(ctx context.Context) (time.Time, error) {
	data, err := o.request(ctx, http.MethodGet, "/api/v5/public/time", nil, false)
	if err != nil {
		return time.Time{}, err
	}
	var rows []struct {
		TS string `json:"ts"`
	}
	if err := json.Unmarshal(data, &rows); err != nil || len(rows) == 0 {
		return time.Time{}, fmt.Errorf("parse server time")
	}
	ms, _ := strconv.ParseInt(rows[0].TS, 10, 64)
	return time.UnixMilli(ms), nil
}

func (o *OKX) swapInstruments(ctx context.Context) ([]okxInstrument, error) {
	data, err := o.request(ctx, http.MethodGet, "/api/v5/public/instruments?instType=SWAP", nil, false)
	if err != nil {
		return nil, err
	}
	var instruments []okxInstrument
	if err := json.Unmarshal(data, &instruments); err != nil {
		return nil, err
	}
	return instruments, nil
}

// OKXSwapVolume returns an approximate 24h quote (USDT) volume per SWAP
// instId, for the live USDT-margined contracts only.
func OKXSwapVolume(ctx context.Context) (map[string]float64, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, okxBaseURL+"/api/v5/market/tickers?instType=SWAP", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", okxUA)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var envelope okxResponse
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Code != "0" {
		return nil, fmt.Errorf("okx tickers error: %s", truncate(body, 200))
	}
	var rows []struct {
		InstID   string `json:"instId"`
		Last     string `json:"last"`
		VolCcy24 string `json:"volCcy24h"`
	}
	if err := json.Unmarshal(envelope.Data, &rows); err != nil {
		return nil, err
	}
	volumes := make(map[string]float64)
	for _, r := range rows {
		if !strings.HasSuffix(r.InstID, "-USDT-SWAP") {
			continue
		}
		last, _ := strconv.ParseFloat(r.Last, 64)
		vol, _ := strconv.ParseFloat(r.VolCcy24, 64)
		volumes[r.InstID] = vol * last
	}
	return volumes, nil
}

// okxBar maps a ninjabot timeframe ("1h","15m","4h","1d") to OKX bar format.
func okxBar(period string) string {
	p := strings.ToLower(strings.TrimSpace(period))
	if p == "" {
		return "1H"
	}
	switch p[len(p)-1] {
	case 'h':
		return p[:len(p)-1] + "H"
	case 'd':
		return p[:len(p)-1] + "D"
	case 'w':
		return p[:len(p)-1] + "W"
	default:
		return p
	}
}

// fetchCandlesRaw returns candles oldest-first; the newest entry may be
// incomplete (confirm=0). Volume is converted to base-coin units.
func (o *OKX) fetchCandlesRaw(ctx context.Context, pair, timeframe string, limit int, after, before string) ([]model.Candle, error) {
	bar := okxBar(timeframe)
	q := url.Values{}
	q.Set("instId", pair)
	q.Set("bar", bar)
	if limit > 0 {
		if limit > 300 {
			limit = 300
		}
		q.Set("limit", strconv.Itoa(limit))
	}
	if after != "" {
		q.Set("after", after)
	}
	if before != "" {
		q.Set("before", before)
	}
	data, err := o.request(ctx, http.MethodGet, "/api/v5/market/candles?"+q.Encode(), nil, false)
	if err != nil {
		return nil, err
	}
	var rows [][]interface{}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("parse candles %s: %w", pair, err)
	}
	candles := make([]model.Candle, 0, len(rows))
	for _, rc := range rows {
		if len(rc) < 6 {
			continue
		}
		ts, _ := strconv.ParseInt(fmt.Sprintf("%v", rc[0]), 10, 64)
		// For SWAP: rc[5]=volume in contracts, rc[6]=volume in base ccy
		volIdx := 5
		if len(rc) >= 7 {
			volIdx = 6
		}
		complete := true
		if len(rc) >= 9 {
			complete = fmt.Sprintf("%v", rc[8]) == "1"
		}
		t := time.UnixMilli(ts)
		candles = append(candles, model.Candle{
			Pair:      pair,
			Time:      t,
			UpdatedAt: t,
			Open:      okxFloat(rc[1]),
			High:      okxFloat(rc[2]),
			Low:       okxFloat(rc[3]),
			Close:     okxFloat(rc[4]),
			Volume:    okxFloat(rc[volIdx]),
			Complete:  complete,
			Metadata:  make(map[string]float64),
		})
	}
	// OKX returns newest first; reverse to oldest-first
	for i, j := 0, len(candles)-1; i < j; i, j = i+1, j-1 {
		candles[i], candles[j] = candles[j], candles[i]
	}
	return candles, nil
}

func okxFloat(v interface{}) float64 {
	f, _ := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
	return f
}

func (o *OKX) CandlesByLimit(ctx context.Context, pair, timeframe string, limit int) ([]model.Candle, error) {
	// first page (limit+1 to account for the still-open candle)
	candles, err := o.fetchCandlesRaw(ctx, pair, timeframe, limit+1, "", "")
	if err != nil {
		return nil, err
	}
	// drop incomplete candles
	complete := make([]model.Candle, 0, len(candles))
	for _, c := range candles {
		if c.Complete {
			complete = append(complete, c)
		}
	}
	candles = complete
	// paginate backwards until we have `limit` closed candles
	for page := 0; len(candles) < limit && page < 3 && len(candles) > 0; page++ {
		older, err := o.fetchCandlesRaw(ctx, pair, timeframe, 300, strconv.FormatInt(candles[0].Time.UnixMilli(), 10), "")
		if err != nil || len(older) == 0 {
			break
		}
		var added []model.Candle
		for _, c := range older {
			if c.Complete {
				added = append(added, c)
			}
		}
		if len(added) == 0 {
			break
		}
		candles = append(added, candles...)
	}
	if len(candles) > limit {
		candles = candles[len(candles)-limit:]
	}
	return candles, nil
}

func (o *OKX) CandlesByPeriod(ctx context.Context, pair, timeframe string,
	start, end time.Time) ([]model.Candle, error) {
	var collected []model.Candle
	after := strconv.FormatInt(end.UnixMilli(), 10)
	for page := 0; page < 10; page++ {
		rows, err := o.fetchCandlesRaw(ctx, pair, timeframe, 300, after, "")
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		oldest := rows[0].Time
		for _, c := range rows {
			if !c.Time.Before(start) && !c.Time.After(end) && c.Complete {
				collected = append(collected, c)
			}
		}
		if oldest.Before(start) || oldest.UnixMilli() <= 0 {
			break
		}
		after = strconv.FormatInt(oldest.UnixMilli(), 10)
	}
	// dedupe + sort ascending
	seen := make(map[int64]bool)
	out := make([]model.Candle, 0, len(collected))
	sort.Slice(collected, func(i, j int) bool { return collected[i].Time.Before(collected[j].Time) })
	for _, c := range collected {
		k := c.Time.UnixMilli()
		if !seen[k] {
			seen[k] = true
			out = append(out, c)
		}
	}
	return out, nil
}

func (o *OKX) CandlesSubscription(ctx context.Context, pair, timeframe string) (chan model.Candle, chan error) {
	ccandle := make(chan model.Candle)
	cerr := make(chan error)

	go func() {
		defer close(ccandle)
		defer close(cerr)

		// stagger pollers so 50 pairs do not hit OKX at once
		n := o.subCounter.Add(1) - 1
		stagger := time.Duration(n%60) * 1200 * time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(stagger):
		}

		var lastEmitted int64 // unix millis of last emitted closed candle
		// seed from history without emitting
		if seed, err := o.fetchCandlesRaw(ctx, pair, timeframe, 5, "", ""); err == nil && len(seed) > 0 {
			for _, c := range seed {
				if c.Complete && c.Time.UnixMilli() > lastEmitted {
					lastEmitted = c.Time.UnixMilli()
				}
			}
		}

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				candles, err := o.fetchCandlesRaw(ctx, pair, timeframe, 5, "", "")
				if err != nil {
					select {
					case cerr <- err:
					default:
					}
					continue
				}
				for _, c := range candles { // oldest -> newest
					if !c.Complete || c.Time.UnixMilli() <= lastEmitted {
						continue
					}
					lastEmitted = c.Time.UnixMilli()
					select {
					case ccandle <- c:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ccandle, cerr
}

func (o *OKX) LastQuote(ctx context.Context, pair string) (float64, error) {
	data, err := o.request(ctx, http.MethodGet, "/api/v5/market/ticker?instId="+url.QueryEscape(pair), nil, false)
	if err != nil {
		return 0, err
	}
	var rows []struct {
		Last string `json:"last"`
	}
	if err := json.Unmarshal(data, &rows); err != nil || len(rows) == 0 {
		return 0, fmt.Errorf("no ticker data for %s", pair)
	}
	return strconv.ParseFloat(rows[0].Last, 64)
}

func (o *OKX) AssetsInfo(pair string) model.AssetInfo {
	return o.assetsInfo[pair]
}

// ---------------------------------------------------------------- quantities

// toContracts converts a coin quantity to OKX contract count, rounded down to
// lotSz and validated against minSz.
func (o *OKX) toContracts(pair string, quantity float64) (float64, error) {
	ctVal := o.ctVal[pair]
	if ctVal <= 0 {
		return 0, ErrInvalidAsset
	}
	lotSz := o.lotSz[pair]
	contracts := math.Floor(quantity/ctVal/lotSz+1e-9) * lotSz
	contracts = math.Round(contracts*math.Pow10(o.lotPrec[pair]+2)) / math.Pow10(o.lotPrec[pair]+2)
	if contracts < o.minSz[pair] {
		return 0, &OrderError{
			Err:      fmt.Errorf("%w: min size %.4f contracts (%.8f %s), got %.4f contracts", ErrInvalidQuantity, o.minSz[pair], o.minSz[pair]*ctVal, pair, quantity/ctVal),
			Pair:     pair,
			Quantity: quantity,
		}
	}
	return contracts, nil
}

func (o *OKX) contractsToCoins(pair string, contracts float64) float64 {
	return contracts * o.ctVal[pair]
}

func (o *OKX) formatPrice(pair string, value float64) string {
	return strconv.FormatFloat(value, 'f', o.tickPrec[pair], 64)
}

func (o *OKX) formatContracts(pair string, contracts float64) string {
	return strconv.FormatFloat(contracts, 'f', o.lotPrec[pair], 64)
}

func okxSide(side model.SideType) string {
	return strings.ToLower(string(side))
}

func okxOrderType(ordType string, hasSL, hasTP bool) model.OrderType {
	switch ordType {
	case "limit":
		return model.OrderTypeLimit
	case "market":
		return model.OrderTypeMarket
	case "move_order_stop":
		return model.OrderTypeTrailingStopMarket
	case "conditional", "oco", "trigger":
		if hasSL && hasTP {
			return model.OrderTypeStopLossLimit
		}
		if hasTP {
			return model.OrderTypeTakeProfit
		}
		return model.OrderTypeStopLoss
	}
	return model.OrderType(ordType)
}

func okxOrderStatus(state string, accFillSz, sz float64) model.OrderStatusType {
	switch state {
	case "live":
		if accFillSz > 0 && accFillSz < sz {
			return model.OrderStatusTypePartiallyFilled
		}
		return model.OrderStatusTypeNew
	case "partially_filled":
		return model.OrderStatusTypePartiallyFilled
	case "filled":
		return model.OrderStatusTypeFilled
	case "effective", "canceled", "partially_canceled":
		return model.OrderStatusTypeCanceled
	}
	return model.OrderStatusTypeNew
}

func (o *OKX) rememberOrderID(exchangeID int64, okxID string, algo bool) {
	o.ordIDs.Store(exchangeID, okxOrderRef{OKXID: okxID, Algo: algo})
}

func (o *OKX) lookupOrderID(exchangeID int64) (string, bool, bool) {
	if v, ok := o.ordIDs.Load(exchangeID); ok {
		ref := v.(okxOrderRef)
		return ref.OKXID, ref.Algo, true
	}
	return strconv.FormatInt(exchangeID, 10), false, false
}

// ---------------------------------------------------------------- trading

type okxOrderAck struct {
	OrdID   string `json:"ordId"`
	AlgoID  string `json:"algoId"`
	ClOrdID string `json:"clOrdId"`
	SCode   string `json:"sCode"`
	SMsg    string `json:"sMsg"`
}

func checkAck(data json.RawMessage) (*okxOrderAck, error) {
	var acks []okxOrderAck
	if err := json.Unmarshal(data, &acks); err != nil || len(acks) == 0 {
		return nil, fmt.Errorf("empty order response: %s", truncate(data, 200))
	}
	if acks[0].SCode != "0" {
		return nil, fmt.Errorf("order rejected: %s", acks[0].SMsg)
	}
	return &acks[0], nil
}

func (o *OKX) CreateOrderOCO(_ model.SideType, _ string, _, _, _, _ float64) ([]model.Order, error) {
	panic("not implemented")
}

// CreateOrderStopTP: 一张OCO同时挂SL+TP(全平, reduceOnly)——OKX一仓只许一张TP/SL单(51088),
// SL/TP同号约定同CreateOrderStop: 负值=buy侧(空头仓保护)
func (o *OKX) CreateOrderStopTP(pair string, quantity float64, sl float64, tp float64) (model.Order, error) {
	side := model.SideTypeSell
	if sl < 0 {
		side = model.SideTypeBuy
		sl = -sl
		tp = -tp
	}
	body := map[string]interface{}{
		"instId":      pair,
		"tdMode":      "isolated",
		"side":        okxSide(side),
		"ordType":     "oco",
		"slTriggerPx": o.formatPrice(pair, sl),
		"slOrdPx":     "-1",
		"tpTriggerPx": o.formatPrice(pair, tp),
		"tpOrdPx":     "-1",
		"reduceOnly":  true,
	}
	if quantity > 0 {
		contracts, err := o.toContracts(pair, quantity)
		if err != nil {
			return model.Order{}, err
		}
		body["sz"] = o.formatContracts(pair, contracts)
	} else {
		body["closeFraction"] = "1"
	}
	data, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/order-algo", body, true)
	if err != nil {
		return model.Order{}, err
	}
	ack, err := checkAck(data)
	if err != nil {
		return model.Order{}, err
	}
	id, _ := strconv.ParseInt(ack.AlgoID, 10, 64)
	o.rememberOrderID(id, ack.AlgoID, true)
	return model.Order{
		ExchangeID:    id,
		ClientOrderID: ack.AlgoID,
		Pair:          pair,
		Side:          side,
		Type:          model.OrderTypeTakeProfit,
		Status:        model.OrderStatusTypeNew,
		Price:         tp,
		Quantity:      quantity,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

func (o *OKX) CreateOrderLimit(side model.SideType, pair string, quantity float64, limit float64) (model.Order, error) {
	contracts, err := o.toContracts(pair, quantity)
	if err != nil {
		return model.Order{}, err
	}
	body := map[string]interface{}{
		"instId":  pair,
		"tdMode":  "isolated",
		"side":    okxSide(side),
		"ordType": "limit",
		"px":      o.formatPrice(pair, limit),
		"sz":      o.formatContracts(pair, contracts),
	}
	data, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/order", body, true)
	if err != nil {
		return model.Order{}, err
	}
	ack, err := checkAck(data)
	if err != nil {
		return model.Order{}, err
	}
	id, _ := strconv.ParseInt(ack.OrdID, 10, 64)
	o.rememberOrderID(id, ack.OrdID, false)
	return model.Order{
		ExchangeID:    id,
		ClientOrderID: ack.ClOrdID,
		Pair:          pair,
		Side:          side,
		Type:          model.OrderTypeLimit,
		Status:        model.OrderStatusTypeNew,
		Price:         limit,
		Quantity:      quantity,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

func (o *OKX) CreateOrderMarket(side model.SideType, pair string, quantity float64, reduceOnly bool) (model.Order, error) {
	contracts, err := o.toContracts(pair, quantity)
	if err != nil {
		return model.Order{}, err
	}
	body := map[string]interface{}{
		"instId":  pair,
		"tdMode":  "isolated",
		"side":    okxSide(side),
		"ordType": "market",
		"sz":      o.formatContracts(pair, contracts),
	}
	if reduceOnly {
		body["reduceOnly"] = true
	}
	data, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/order", body, true)
	if err != nil {
		return model.Order{}, err
	}
	ack, err := checkAck(data)
	if err != nil {
		return model.Order{}, err
	}
	id, _ := strconv.ParseInt(ack.OrdID, 10, 64)
	o.rememberOrderID(id, ack.OrdID, false)

	// market orders fill near-instantly; poll briefly for the fill price
	order := model.Order{
		ExchangeID:    id,
		ClientOrderID: ack.ClOrdID,
		Pair:          pair,
		Side:          side,
		Type:          model.OrderTypeMarket,
		Status:        model.OrderStatusTypeNew,
		Quantity:      quantity,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	for i := 0; i < 5; i++ {
		time.Sleep(200 * time.Millisecond)
		fetched, err := o.Order(pair, id)
		if err != nil {
			continue
		}
		if fetched.Status == model.OrderStatusTypeFilled && fetched.Price > 0 {
			fetched.ClientOrderID = order.ClientOrderID
			return fetched, nil
		}
		if fetched.Status != model.OrderStatusTypeNew || fetched.Price > 0 {
			return fetched, nil
		}
	}
	return order, nil
}

// CreateOrderStop mirrors Binance STOP_MARKET: limit > 0 places a sell stop
// below price (close long), limit < 0 places a buy stop above price (close
// short). quantity == 0 closes the whole position when triggered.
func (o *OKX) CreateOrderStop(pair string, quantity float64, limit float64) (model.Order, error) {
	side := model.SideTypeSell
	if limit < 0 {
		side = model.SideTypeBuy
		limit = -limit
	}

	body := map[string]interface{}{
		"instId":      pair,
		"tdMode":      "isolated",
		"side":        okxSide(side),
		"ordType":     "conditional",
		"slTriggerPx": o.formatPrice(pair, limit),
		"slOrdPx":     "-1", // market when triggered
		"reduceOnly":  true,
	}
	if quantity > 0 {
		contracts, err := o.toContracts(pair, quantity)
		if err != nil {
			return model.Order{}, err
		}
		body["sz"] = o.formatContracts(pair, contracts)
	} else {
		body["closeFraction"] = "1"
	}

	data, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/order-algo", body, true)
	if err != nil {
		return model.Order{}, err
	}
	ack, err := checkAck(data)
	if err != nil {
		return model.Order{}, err
	}
	id, _ := strconv.ParseInt(ack.AlgoID, 10, 64)
	o.rememberOrderID(id, ack.AlgoID, true)
	return model.Order{
		ExchangeID:    id,
		ClientOrderID: ack.AlgoID,
		Pair:          pair,
		Side:          side,
		Type:          model.OrderTypeStopLoss,
		Status:        model.OrderStatusTypeNew,
		Price:         limit,
		Quantity:      quantity,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

// TakeProfit mirrors Binance TAKE_PROFIT / TAKE_PROFIT_MARKET. `side` is the
// closing side (SELL closes a long). quantity == 0 closes the whole position.
func (o *OKX) TakeProfit(side model.SideType, pair string, quantity float64, limit float64) (model.Order, error) {
	body := map[string]interface{}{
		"instId":      pair,
		"tdMode":      "isolated",
		"side":        okxSide(side),
		"ordType":     "conditional",
		"tpTriggerPx": o.formatPrice(pair, limit),
		"reduceOnly":  true,
	}
	if quantity > 0 {
		contracts, err := o.toContracts(pair, quantity)
		if err != nil {
			return model.Order{}, err
		}
		body["sz"] = o.formatContracts(pair, contracts)
		body["tpOrdPx"] = o.formatPrice(pair, limit)
	} else {
		body["tpOrdPx"] = "-1"
		body["closeFraction"] = "1"
	}

	data, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/order-algo", body, true)
	if err != nil {
		return model.Order{}, err
	}
	ack, err := checkAck(data)
	if err != nil {
		return model.Order{}, err
	}
	id, _ := strconv.ParseInt(ack.AlgoID, 10, 64)
	o.rememberOrderID(id, ack.AlgoID, true)
	return model.Order{
		ExchangeID:    id,
		ClientOrderID: ack.AlgoID,
		Pair:          pair,
		Side:          side,
		Type:          model.OrderTypeTakeProfit,
		Status:        model.OrderStatusTypeNew,
		Price:         limit,
		Quantity:      quantity,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

// CreateOrderTrailingStop mirrors Binance TRAILING_STOP_MARKET.
// callBackRate is a percent string ("2" = 2%); activation `limit` is the
// activation price. `side` is the closing side.
func (o *OKX) CreateOrderTrailingStop(pair string, side model.SideType, limit float64, quantity float64, callBackRate string) (model.Order, error) {
	rate, err := strconv.ParseFloat(strings.TrimSpace(callBackRate), 64)
	if err != nil {
		return model.Order{}, fmt.Errorf("invalid callback rate %q: %w", callBackRate, err)
	}
	ratio := rate / 100.0
	if ratio <= 0 || ratio >= 1 {
		ratio = 0.02
	}
	body := map[string]interface{}{
		"instId":        pair,
		"tdMode":        "isolated",
		"side":          okxSide(side),
		"ordType":       "move_order_stop",
		"activePx":      o.formatPrice(pair, limit),
		"callbackRatio": strconv.FormatFloat(ratio, 'f', -1, 64),
		"reduceOnly":    true,
	}
	if quantity > 0 {
		contracts, err := o.toContracts(pair, quantity)
		if err != nil {
			return model.Order{}, err
		}
		body["sz"] = o.formatContracts(pair, contracts)
	} else {
		body["closeFraction"] = "1"
	}

	data, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/order-algo", body, true)
	if err != nil {
		return model.Order{}, err
	}
	ack, err := checkAck(data)
	if err != nil {
		return model.Order{}, err
	}
	id, _ := strconv.ParseInt(ack.AlgoID, 10, 64)
	o.rememberOrderID(id, ack.AlgoID, true)
	return model.Order{
		ExchangeID:    id,
		ClientOrderID: ack.AlgoID,
		Pair:          pair,
		Side:          side,
		Type:          model.OrderTypeTrailingStopMarket,
		Status:        model.OrderStatusTypeNew,
		Price:         limit,
		Quantity:      quantity,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

// CreateAlgoTrailingStop is the klnt-strategy helper for raw trailing stops
// (ordType=move_order_stop). side: "buy"/"sell" (closing side).
// callbackRatio is a decimal fraction, e.g. 0.02 = 2%.
func (o *OKX) CreateAlgoTrailingStop(pair string, side string, activationPx, quantity float64, callbackRatio float64) error {
	contracts, err := o.toContracts(pair, quantity)
	if err != nil {
		return err
	}
	side = strings.ToLower(strings.TrimSpace(side))
	if side == "" {
		side = "sell"
	}
	if callbackRatio <= 0 || callbackRatio >= 1 {
		callbackRatio = 0.02
	}
	body := map[string]interface{}{
		"instId":        pair,
		"tdMode":        "isolated",
		"side":          side,
		"ordType":       "move_order_stop",
		"sz":            o.formatContracts(pair, contracts),
		"activePx":      o.formatPrice(pair, activationPx),
		"callbackRatio": strconv.FormatFloat(callbackRatio, 'f', -1, 64),
		"reduceOnly":    true,
	}
	data, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/order-algo", body, true)
	if err != nil {
		return err
	}
	ack, err := checkAck(data)
	if err != nil {
		return err
	}
	log.Infof("[OKX] algo trailing stop placed: pair=%s side=%s activePx=%s ratio=%.4f algoId=%s",
		pair, side, o.formatPrice(pair, activationPx), callbackRatio, ack.AlgoID)
	return nil
}

func (o *OKX) CreateOrderMarketQuote(_ model.SideType, _ string, _ float64) (model.Order, error) {
	panic("not implemented")
}

// ---------------------------------------------------------------- order state

type okxOrderRow struct {
	OrdID       string `json:"ordId"`
	AlgoID      string `json:"algoId"`
	ClOrdID     string `json:"clOrdId"`
	InstID      string `json:"instId"`
	Side        string `json:"side"`
	OrdType     string `json:"ordType"`
	Px          string `json:"px"`
	Sz          string `json:"sz"`
	FillSz      string `json:"accFillSz"`
	AvgPx       string `json:"avgPx"`
	SlTriggerPx string `json:"slTriggerPx"`
	TpTriggerPx string `json:"tpTriggerPx"`
	ActivePx    string `json:"activePx"`
	State       string `json:"state"`
	CTime       string `json:"cTime"`
	UTime       string `json:"uTime"`
}

func (o *OKX) rowToOrder(row okxOrderRow, algo bool) model.Order {
	sz, _ := strconv.ParseFloat(row.Sz, 64)
	fillSz, _ := strconv.ParseFloat(row.FillSz, 64)
	avgPx, _ := strconv.ParseFloat(row.AvgPx, 64)
	px, _ := strconv.ParseFloat(row.Px, 64)
	idStr := row.OrdID
	if algo {
		idStr = row.AlgoID
	}
	id, _ := strconv.ParseInt(idStr, 10, 64)
	ct, _ := strconv.ParseInt(row.CTime, 10, 64)
	ut, _ := strconv.ParseInt(row.UTime, 10, 64)
	if ct == 0 {
		ct = time.Now().UnixMilli()
	}
	if ut == 0 {
		ut = ct
	}

	hasSL := row.SlTriggerPx != ""
	hasTP := row.TpTriggerPx != ""
	orderType := okxOrderType(row.OrdType, hasSL, hasTP)
	if algo && row.OrdType == "move_order_stop" {
		orderType = model.OrderTypeTrailingStopMarket
	}

	price := px
	if price == 0 {
		price = avgPx
	}
	if price == 0 {
		if hasSL {
			price, _ = strconv.ParseFloat(row.SlTriggerPx, 64)
		} else if hasTP {
			price, _ = strconv.ParseFloat(row.TpTriggerPx, 64)
		}
	}

	// state for algo rows: "effective" means the trigger fired
	status := okxOrderStatus(row.State, fillSz, sz)
	if algo && row.State == "effective" {
		status = model.OrderStatusTypeFilled
	}

	qty := o.contractsToCoins(row.InstID, sz)
	if qty == 0 && fillSz > 0 {
		qty = o.contractsToCoins(row.InstID, fillSz)
	}
	if fillSz > 0 && avgPx > 0 {
		// report actual filled quantity for market orders
		qty = o.contractsToCoins(row.InstID, fillSz)
	}

	return model.Order{
		ExchangeID:    id,
		ClientOrderID: row.ClOrdID,
		Pair:          row.InstID,
		CreatedAt:     time.UnixMilli(ct),
		UpdatedAt:     time.UnixMilli(ut),
		Side:          model.SideType(strings.ToUpper(row.Side)),
		Type:          orderType,
		Status:        status,
		Price:         price,
		Quantity:      qty,
	}
}

func (o *OKX) Order(pair string, id int64) (model.Order, error) {
	okxID, algo, _ := o.lookupOrderID(id)
	if algo {
		for _, ordType := range []string{"conditional", "move_order_stop", "oco"} {
			path := fmt.Sprintf("/api/v5/trade/orders-algo-pending?ordType=%s&algoId=%s&instId=%s",
				ordType, url.QueryEscape(okxID), url.QueryEscape(pair))
			data, err := o.request(o.ctx, http.MethodGet, path, nil, true)
			if err != nil {
				continue
			}
			var rows []okxOrderRow
			if err := json.Unmarshal(data, &rows); err == nil && len(rows) > 0 {
				return o.rowToOrder(rows[0], true), nil
			}
		}
		for _, ordType := range []string{"conditional", "move_order_stop", "oco"} {
			path := fmt.Sprintf("/api/v5/trade/orders-algo-history?ordType=%s&algoId=%s&instId=%s",
				ordType, url.QueryEscape(okxID), url.QueryEscape(pair))
			data, err := o.request(o.ctx, http.MethodGet, path, nil, true)
			if err != nil {
				continue
			}
			var rows []okxOrderRow
			if err := json.Unmarshal(data, &rows); err == nil && len(rows) > 0 {
				return o.rowToOrder(rows[0], true), nil
			}
		}
		return model.Order{}, fmt.Errorf("algo order %s not found for %s", okxID, pair)
	}

	path := fmt.Sprintf("/api/v5/trade/order?instId=%s&ordId=%s", url.QueryEscape(pair), url.QueryEscape(okxID))
	data, err := o.request(o.ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return model.Order{}, err
	}
	var rows []okxOrderRow
	if err := json.Unmarshal(data, &rows); err != nil || len(rows) == 0 {
		return model.Order{}, fmt.Errorf("order %s not found for %s", okxID, pair)
	}
	return o.rowToOrder(rows[0], false), nil
}

func (o *OKX) OpenOrders(pair string) ([]model.Order, error) {
	orders := make([]model.Order, 0)

	// regular pending orders (instId optional: omitted => all instruments)
	pendingPath := "/api/v5/trade/orders-pending"
	if pair != "" {
		pendingPath += "?instId=" + url.QueryEscape(pair)
	}
	data, err := o.request(o.ctx, http.MethodGet, pendingPath, nil, true)
	if err == nil {
		var rows []okxOrderRow
		if err := json.Unmarshal(data, &rows); err == nil {
			for _, row := range rows {
				orders = append(orders, o.rowToOrder(row, false))
			}
		}
	}

	// pending algo orders (stops / trailing stops / TP)
	for _, ordType := range []string{"conditional", "move_order_stop", "oco"} {
		path := fmt.Sprintf("/api/v5/trade/orders-algo-pending?ordType=%s", ordType)
		if pair != "" {
			path += "&instId=" + url.QueryEscape(pair)
		}
		data, err := o.request(o.ctx, http.MethodGet, path, nil, true)
		if err != nil {
			continue
		}
		var rows []okxOrderRow
		if err := json.Unmarshal(data, &rows); err == nil {
			for _, row := range rows {
				orders = append(orders, o.rowToOrder(row, true))
			}
		}
	}

	return orders, nil
}

func (o *OKX) Orders(pair string, limit int) ([]model.Order, error) {
	if limit > 100 {
		limit = 100
	}
	data, err := o.request(o.ctx, http.MethodGet,
		fmt.Sprintf("/api/v5/trade/orders-history?instId=%s&limit=%d", url.QueryEscape(pair), limit), nil, true)
	if err != nil {
		return nil, err
	}
	var rows []okxOrderRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	orders := make([]model.Order, 0, len(rows))
	for _, row := range rows {
		orders = append(orders, o.rowToOrder(row, false))
	}
	return orders, nil
}

func (o *OKX) Cancel(order model.Order) error {
	okxID, algo, _ := o.lookupOrderID(order.ExchangeID)
	if algo {
		body := []map[string]interface{}{{"algoId": okxID, "instId": order.Pair}}
		_, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/cancel-algos", body, true)
		return err
	}
	body := map[string]interface{}{"instId": order.Pair, "ordId": okxID}
	_, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/cancel-order", body, true)
	return err
}

func (o *OKX) CancelOpenOrders(pair string) error {
	// regular orders via batch cancel
	if data, err := o.request(o.ctx, http.MethodGet,
		"/api/v5/trade/orders-pending?instId="+url.QueryEscape(pair), nil, true); err == nil {
		var rows []okxOrderRow
		if err := json.Unmarshal(data, &rows); err == nil && len(rows) > 0 {
			reqs := make([]map[string]interface{}, 0, len(rows))
			for _, row := range rows {
				reqs = append(reqs, map[string]interface{}{"instId": pair, "ordId": row.OrdID})
			}
			if _, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/cancel-batch-orders", reqs, true); err != nil {
				return fmt.Errorf("cancel regular orders for %s: %w", pair, err)
			}
		}
	}

	// algo orders (stop / trailing / TP)
	for _, ordType := range []string{"conditional", "move_order_stop", "oco"} {
		data, err := o.request(o.ctx, http.MethodGet,
			fmt.Sprintf("/api/v5/trade/orders-algo-pending?ordType=%s&instId=%s", ordType, url.QueryEscape(pair)), nil, true)
		if err != nil {
			continue
		}
		var rows []okxOrderRow
		if err := json.Unmarshal(data, &rows); err != nil || len(rows) == 0 {
			continue
		}
		reqs := make([]map[string]interface{}, 0, len(rows))
		for _, row := range rows {
			reqs = append(reqs, map[string]interface{}{"algoId": row.AlgoID, "instId": pair})
		}
		if _, err := o.request(o.ctx, http.MethodPost, "/api/v5/trade/cancel-algos", reqs, true); err != nil {
			return fmt.Errorf("cancel algo orders (%s) for %s: %w", ordType, pair, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------- account

func (o *OKX) Account() (model.Account, error) {
	o.accountMu.Lock()
	if o.accountData != nil && time.Since(o.accountTime) < 3*time.Second {
		cached := *o.accountData
		o.accountMu.Unlock()
		return cached, nil
	}
	o.accountMu.Unlock()

	data, err := o.request(o.ctx, http.MethodGet, "/api/v5/account/balance", nil, true)
	if err != nil {
		return model.Account{}, err
	}
	var rows []struct {
		TotalEq string `json:"totalEq"`
		AvailEq string `json:"availEq"`
		Details []struct {
			Ccy      string `json:"ccy"`
			AvailBal string `json:"availBal"`
			AvailEq  string `json:"availEq"`
			Eq       string `json:"eq"`
			Frozen   string `json:"frozen"`
		} `json:"details"`
	}
	if err := json.Unmarshal(data, &rows); err != nil || len(rows) == 0 {
		return model.Account{}, fmt.Errorf("parse balance")
	}
	balances := make([]model.Balance, 0)
	acc := rows[0]
	for _, d := range acc.Details {
		if d.Ccy != "USDT" {
			continue
		}
		free, _ := strconv.ParseFloat(d.AvailEq, 64)
		if free == 0 {
			free, _ = strconv.ParseFloat(d.AvailBal, 64)
		}
		eq, _ := strconv.ParseFloat(d.Eq, 64)
		if free == 0 && eq == 0 {
			continue
		}
		balances = append(balances, model.Balance{
			Asset: d.Ccy,
			Free:  free,
			Lock:  math.Max(eq-free, 0),
		})
	}
	avail, _ := strconv.ParseFloat(acc.AvailEq, 64)

	// positions (net mode: pos is signed contracts)
	posData, err := o.request(o.ctx, http.MethodGet, "/api/v5/account/positions", nil, true)
	if err == nil {
		var positions []struct {
			InstID string `json:"instId"`
			Pos    string `json:"pos"`
			Lever  string `json:"lever"`
			AvgPx  string `json:"avgPx"`
			Upl    string `json:"upl"`
			Margin string `json:"margin"`
		}
		if err := json.Unmarshal(posData, &positions); err == nil {
			for _, p := range positions {
				posContracts, _ := strconv.ParseFloat(p.Pos, 64)
				if posContracts == 0 {
					continue
				}
				lever, _ := strconv.ParseFloat(p.Lever, 64)
				asset, _ := OKXSplitAssetQuote(p.InstID)
				balances = append(balances, model.Balance{
					Asset:    asset,
					Free:     o.contractsToCoins(p.InstID, posContracts), // signed: negative = short
					Leverage: lever,
				})
			}
		}
	}

	account := model.Account{Balances: balances, Available: avail}
	o.accountMu.Lock()
	o.accountData = &account
	o.accountTime = time.Now()
	o.accountMu.Unlock()
	return account, nil
}

// Position(11-08 P0修复): SWAP的持仓在/account/positions(带方向), 余额口径的asset在合约账户恒0——
// 旧实现(读余额)导致策略侧持仓门/出场数量/重启恢复全部拿到0: Sell静默no-op、恢复跳过、可重复开仓。
func (o *OKX) Position(pair string) (asset, quote float64, err error) {
	_, quoteTick := OKXSplitAssetQuote(pair)
	acc, err := o.Account()
	if err != nil {
		return 0, 0, err
	}
	assetTick, quoteTick := OKXSplitAssetQuote(pair)
	_, quoteBalance := acc.Balance(assetTick, quoteTick)
	data, err := o.request(o.ctx, http.MethodGet, "/api/v5/account/positions?instId="+url.QueryEscape(pair), nil, true)
	if err != nil {
		return 0, quoteBalance.Free, err
	}
	var rows []struct {
		Pos string `json:"pos"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, quoteBalance.Free, err
	}
	pos := 0.0
	if len(rows) > 0 {
		pos, _ = strconv.ParseFloat(rows[0].Pos, 64)
	}
	return o.contractsToCoins(pair, pos), quoteBalance.Free, nil
}

// AccountSubscription polls recent order history and emits filled orders,
// replacing the Binance user-data websocket.
func (o *OKX) AccountSubscription(ctx context.Context) (chan model.Order, chan error) {
	orders := make(chan model.Order)
	cerr := make(chan error)

	go func() {
		defer close(orders)
		defer close(cerr)
		seen := make(map[string]bool)
		startMs := time.Now().UnixMilli() // P0修复(11-07): 历史成交回放触发SMCBB幽灵止损26笔
		first := true
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				data, err := o.request(ctx, http.MethodGet,
					"/api/v5/trade/orders-history?instType=SWAP&limit=50", nil, true)
				if err != nil {
					select {
					case cerr <- err:
					default:
					}
					continue
				}
				var rows []okxOrderRow
				if err := json.Unmarshal(data, &rows); err != nil {
					continue
				}
				emitted := make([]model.Order, 0, len(rows))
				for _, row := range rows {
					key := row.OrdID + ":" + row.State
					if seen[key] {
						continue
					}
					seen[key] = true
					ut, _ := strconv.ParseInt(row.UTime, 10, 64)
					if first || ut <= startMs {
						continue // 首轮只建基线; 启动前的历史成交永不发射
					}
					if row.State == "filled" && row.OrdID != "" {
						emitted = append(emitted, o.rowToOrder(row, false))
					}
				}
				first = false
				for _, ord := range emitted {
					select {
					case orders <- ord:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return orders, cerr
}
