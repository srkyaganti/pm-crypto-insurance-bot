package recorder

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type RecorderSnapshot struct {
	TS      int64                 `json:"ts"`
	TsISO   string                `json:"ts_iso"`
	Symbols map[string]SymbolData `json:"symbols"`
	Markets []MarketData          `json:"markets"`
}

type SymbolData struct {
	Chainlink    CLData    `json:"chainlink"`
	BinanceSpot  SpotData  `json:"binance_spot"`
	BinancePerps PerpsData `json:"binance_perps"`
}

type CLData struct {
	Price float64    `json:"price"`
	Stale bool       `json:"stale"`
	Ticks []TickData `json:"ticks"`
}

type TickData struct {
	Price float64 `json:"price"`
	TS    int64   `json:"ts"`
}

type SpotData struct {
	Mid     float64     `json:"mid"`
	BestBid float64     `json:"best_bid"`
	BestAsk float64     `json:"best_ask"`
	Bids    []LevelData `json:"bids"`
	Asks    []LevelData `json:"asks"`
	Stale   bool        `json:"stale"`
}

type LevelData struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type PerpsData struct {
	MarkPrice       float64 `json:"mark_price"`
	LiquidationFlag bool    `json:"liquidation_flag"`
	Stale           bool    `json:"stale"`
}

type PMLevelData struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type MarketData struct {
	Slug           string        `json:"slug"`
	Symbol         string        `json:"symbol"`
	StartPrice     float64       `json:"start_price"`
	TimeRemainingS int64         `json:"time_remaining_s"`
	YesBid         float64       `json:"yes_bid"`
	YesAsk         float64       `json:"yes_ask"`
	NoBid          float64       `json:"no_bid"`
	NoAsk          float64       `json:"no_ask"`
	YesBids        []PMLevelData `json:"yes_bids,omitempty"`
	YesAsks        []PMLevelData `json:"yes_asks,omitempty"`
	NoBids         []PMLevelData `json:"no_bids,omitempty"`
	NoAsks         []PMLevelData `json:"no_asks,omitempty"`
}

// S3Config holds optional S3 archival settings.
type S3Config struct {
	Enabled  bool
	Bucket   string
	Region   string
	Prefix   string
}

type Recorder struct {
	state      *state.StateStore
	getWindows func() []tracker.ActiveWindowSummary
	symbols    []string
	interval   time.Duration
	outputDir  string
	s3         S3Config
	s3Client   *s3.Client
	logger     zerolog.Logger

	currentFile  *os.File
	currentSlot  int64 // unique slot ID for 15-minute rotation
	currentFname string
	enc          *json.Encoder
	buf          *bufio.Writer
	tickCount    int
}

func New(
	store *state.StateStore,
	getWindows func() []tracker.ActiveWindowSummary,
	symbols []string,
	interval time.Duration,
	outputDir string,
	s3 S3Config,
	logger *zerolog.Logger,
) (*Recorder, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("create recorder output dir: %w", err)
	}

	return &Recorder{
		state:       store,
		getWindows:  getWindows,
		symbols:     symbols,
		interval:    interval,
		outputDir:   outputDir,
		s3:          s3,
		logger:      logger.With().Str("component", "recorder").Logger(),
		currentSlot: -1,
	}, nil
}

func (r *Recorder) Run(ctx context.Context) error {
	r.logger.Info().
		Str("output_dir", r.outputDir).
		Dur("interval", r.interval).
		Strs("symbols", r.symbols).
		Bool("s3_enabled", r.s3.Enabled).
		Msg("recorder starting")

	if r.s3.Enabled {
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion(r.s3.Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				os.Getenv("AWS_ACCESS_KEY_ID"),
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				"",
			)),
		)
		if err != nil {
			return fmt.Errorf("load AWS config: %w", err)
		}
		r.s3Client = s3.NewFromConfig(cfg)
		r.logger.Info().
			Str("bucket", r.s3.Bucket).
			Str("region", r.s3.Region).
			Str("prefix", r.s3.Prefix).
			Msg("s3 archival enabled")
		go r.archiveLoop(ctx)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info().Msg("recorder stopping")
			r.flush()
			r.closeFile()
			return nil
		case <-ticker.C:
			r.capture()
		}
	}
}

func (r *Recorder) Stop() {
	r.flush()
	r.closeFile()
}

func (r *Recorder) capture() {
	now := time.Now().UTC()

	if err := r.rotateIfNeeded(now); err != nil {
		r.logger.Error().Err(err).Msg("failed to rotate file")
		return
	}

	if r.enc == nil {
		return
	}

	snap := RecorderSnapshot{
		TS:      now.UnixMilli(),
		TsISO:   now.Format(time.RFC3339Nano),
		Symbols: make(map[string]SymbolData, len(r.symbols)),
	}

	for _, sym := range r.symbols {
		cl := r.state.GetChainlink(sym)
		sp := r.state.GetBinanceSpot(sym)
		pp := r.state.GetBinancePerps(sym)

		// Convert CL ring buffer ticks
		allTicks := cl.Ticks.All()
		ticks := make([]TickData, len(allTicks))
		for i, t := range allTicks {
			ticks[i] = TickData{
				Price: t.Price,
				TS:    t.Timestamp.UnixMilli(),
			}
		}

		// Convert Binance spot order book levels
		bids := make([]LevelData, len(sp.OrderBook.Bids))
		for i, l := range sp.OrderBook.Bids {
			bids[i] = LevelData{Price: l.Price, Size: l.Size}
		}
		asks := make([]LevelData, len(sp.OrderBook.Asks))
		for i, l := range sp.OrderBook.Asks {
			asks[i] = LevelData{Price: l.Price, Size: l.Size}
		}

		snap.Symbols[sym] = SymbolData{
			Chainlink: CLData{
				Price: cl.LatestPrice,
				Stale: cl.IsStale,
				Ticks: ticks,
			},
			BinanceSpot: SpotData{
				Mid:     sp.OrderBook.MidPrice,
				BestBid: sp.OrderBook.BestBid,
				BestAsk: sp.OrderBook.BestAsk,
				Bids:    bids,
				Asks:    asks,
				Stale:   sp.IsStale,
			},
			BinancePerps: PerpsData{
				MarkPrice:       pp.LastMarkPrice,
				LiquidationFlag: pp.LiquidationFlag,
				Stale:           pp.IsStale,
			},
		}
	}

	// PM markets
	windows := r.getWindows()
	pmState := r.state.GetPolymarket()
	markets := make([]MarketData, len(windows))
	for i, w := range windows {
		md := MarketData{
			Slug:           w.Slug,
			Symbol:         w.Symbol,
			StartPrice:     w.StartPrice,
			TimeRemainingS: w.TimeRemainingS,
			YesBid:         w.YesBid,
			YesAsk:         w.YesAsk,
			NoBid:          w.NoBid,
			NoAsk:          w.NoAsk,
		}

		if ob := pmState.OrderBooks[w.YesAssetID]; ob != nil {
			md.YesBids = convertPMLevels(ob.Bids)
			md.YesAsks = convertPMLevels(ob.Asks)
		}
		if ob := pmState.OrderBooks[w.NoAssetID]; ob != nil {
			md.NoBids = convertPMLevels(ob.Bids)
			md.NoAsks = convertPMLevels(ob.Asks)
		}

		markets[i] = md
	}
	snap.Markets = markets

	if err := r.enc.Encode(snap); err != nil {
		r.logger.Error().Err(err).Msg("failed to encode snapshot")
		return
	}

	r.tickCount++
	if r.tickCount%5 == 0 {
		r.flush()
	}
}

// slotID returns a unique int for the 15-minute slot containing the given time.
func slotID(t time.Time) int64 {
	return t.Unix() / (15 * 60)
}

// slotFilename returns the filename for the 15-minute slot containing the given time.
// Format: data-2026-03-19-08-15.ndjson (the minute is the slot start: 00, 15, 30, 45).
func slotFilename(t time.Time) string {
	slotStart := t.Truncate(15 * time.Minute)
	return fmt.Sprintf("data-%s.ndjson", slotStart.Format("2006-01-02-15-04"))
}

func (r *Recorder) rotateIfNeeded(now time.Time) error {
	slot := slotID(now)
	if r.currentFile != nil && r.currentSlot == slot {
		return nil
	}

	r.flush()
	r.closeFile()

	filename := slotFilename(now)
	path := filepath.Join(r.outputDir, filename)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open recorder file %s: %w", path, err)
	}

	r.currentFile = f
	r.currentSlot = slot
	r.currentFname = filename
	r.buf = bufio.NewWriter(f)
	r.enc = json.NewEncoder(r.buf)

	r.logger.Info().Str("file", filename).Msg("opened new recorder file")
	return nil
}

func (r *Recorder) flush() {
	if r.buf != nil {
		if err := r.buf.Flush(); err != nil {
			r.logger.Error().Err(err).Msg("failed to flush recorder buffer")
		}
	}
}

func convertPMLevels(levels []state.PMOrderLevel) []PMLevelData {
	if len(levels) == 0 {
		return nil
	}
	out := make([]PMLevelData, len(levels))
	for i, l := range levels {
		out[i] = PMLevelData{Price: l.Price, Size: l.Size}
	}
	return out
}

func (r *Recorder) closeFile() {
	if r.currentFile != nil {
		if err := r.currentFile.Close(); err != nil {
			r.logger.Error().Err(err).Msg("failed to close recorder file")
		}
		r.currentFile = nil
		r.enc = nil
		r.buf = nil
	}
}

// archiveLoop runs every 5 minutes, uploading completed (non-current) NDJSON files to S3.
func (r *Recorder) archiveLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.archiveOnce()
		}
	}
}

func (r *Recorder) archiveOnce() {
	currentFile := slotFilename(time.Now().UTC())

	pattern := filepath.Join(r.outputDir, "data-*.ndjson")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		r.logger.Error().Err(err).Msg("s3 archive: glob failed")
		return
	}

	sort.Strings(matches)

	archived := 0
	for _, path := range matches {
		name := filepath.Base(path)

		// Skip the file currently being written.
		if name == currentFile {
			continue
		}

		info, err := os.Stat(path)
		if err != nil {
			continue
		}

		// Delete empty files.
		if info.Size() == 0 {
			os.Remove(path)
			continue
		}

		s3Key := r.s3.Prefix + name
		if err := r.uploadToS3(path, s3Key); err != nil {
			r.logger.Error().Err(err).Str("file", name).Msg("s3 archive: upload failed")
			continue
		}

		if err := os.Remove(path); err != nil {
			r.logger.Error().Err(err).Str("file", name).Msg("s3 archive: delete after upload failed")
			continue
		}

		archived++
	}

	if archived > 0 {
		r.logger.Info().Int("archived", archived).Msg("s3 archive: completed")
	}
}

func (r *Recorder) uploadToS3(localPath, s3Key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.s3.Bucket,
		Key:    &s3Key,
		Body:   f,
	})
	return err
}
