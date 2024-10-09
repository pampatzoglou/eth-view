package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

// Struct to hold block data
type Block struct {
	Number       string        `json:"number"`
	Hash         string        `json:"hash"`
	Timestamp    string        `json:"timestamp"`
	Transactions []Transaction `json:"transactions"`
}

// Struct to hold transaction data
type Transaction struct {
	Hash  string `json:"hash"`
	From  string `json:"from"`
	To    string `json:"to"`
	Value string `json:"value"`
}

// Provider represents an Ethereum data provider
type Provider struct {
	Name      string
	URL       string
	ProjectID string
}

// Prometheus Metrics
var (
	httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "eth_http_request_duration_seconds",
		Help:    "Duration of HTTP requests to Ethereum providers",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "provider", "status"})

	providerErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "eth_provider_errors_total",
		Help: "Total number of provider errors",
	}, []string{"provider"})

	latestBlockGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "eth_latest_block",
		Help: "Latest block fetched from any provider",
	})

	providerBlockGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "eth_provider_latest_block",
		Help: "Latest block fetched from each provider",
	}, []string{"provider"})

	syncStatusGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "eth_sync_status",
		Help: "Sync status: 0 for syncing, 1 for synced",
	})

	blocksBehindGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "eth_blocks_behind",
		Help: "Number of blocks behind the network",
	})
)

var (
	providers = []Provider{
		{Name: "Infura", URL: "https://mainnet.infura.io/v3/%s", ProjectID: os.Getenv("INFURA_PROJECT_ID")},
		{Name: "Alchemy", URL: "https://eth-mainnet.alchemyapi.io/v2/%s", ProjectID: os.Getenv("ALCHEMY_API_KEY")},
		// Add more providers as needed
	}
)

// Logger setup
var log = logrus.New()

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(httpRequestDuration, providerErrorCounter, latestBlockGauge, providerBlockGauge, syncStatusGauge, blocksBehindGauge)

	// Set up structured logging with Logrus
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(os.Stdout)
}

// Set log level dynamically
func setLogLevel(level string) error {
	parsedLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("invalid log level: %s", level)
	}
	log.SetLevel(parsedLevel)
	log.Infof("Log level set to %s", level)
	return nil
}

// HTTP handler to change log level at runtime
func logLevelHandler(w http.ResponseWriter, r *http.Request) {
	level := strings.TrimPrefix(r.URL.Path, "/log/")
	err := setLogLevel(level)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "Log level changed to %s", level)
}

// Connect to PostgreSQL
func connectDB() (*sql.DB, error) {
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbHost := os.Getenv("DB_HOST")
	devMode := os.Getenv("DEV_MODE")

	if devMode == "true" {
		log.Warn("DEV_MODE enabled: Skipping database connection")
		return nil, nil
	}

	if dbUser == "" || dbPassword == "" || dbName == "" || dbHost == "" {
		return nil, fmt.Errorf("database environment variables missing")
	}

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, dbUser, dbPassword, dbName)
	return sql.Open("postgres", connStr)
}

// Health check handlers
func livenessProbe(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "OK")
}

func readinessProbe(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			http.Error(w, "Database not connected", http.StatusServiceUnavailable)
			return
		}
		err := db.Ping()
		if err != nil {
			http.Error(w, "Database not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "OK")
	}
}

// startupProbe checks if the application has reached the current block height
func startupProbe(threshold uint64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var syncStatus float64
		var blocksBehind float64

		metrics, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			http.Error(w, "Error gathering metrics", http.StatusInternalServerError)
			return
		}

		for _, metric := range metrics {
			if metric.GetName() == "eth_sync_status" {
				syncStatus = *metric.Metric[0].Gauge.Value
			}
			if metric.GetName() == "eth_blocks_behind" {
				blocksBehind = *metric.Metric[0].Gauge.Value
			}
		}

		if syncStatus == 1 || blocksBehind <= float64(threshold) {
			fmt.Fprint(w, "OK")
		} else {
			http.Error(w, fmt.Sprintf("Still syncing, %d blocks behind", int(blocksBehind)), http.StatusServiceUnavailable)
		}
	}
}

// fetchBlockFromProvider fetches the latest block from a provider with exponential backoff
func fetchBlockFromProvider(ctx context.Context, provider Provider) (*Block, error) {
	var block *Block
	operation := func() error {
		url := fmt.Sprintf(provider.URL, provider.ProjectID)
		req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest", true],"id":1}`))
		if err != nil {
			return fmt.Errorf("error creating request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error fetching block: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		var result struct {
			Result Block `json:"result"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}

		block = &result.Result
		return nil
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 1 * time.Minute

	if err := backoff.Retry(operation, b); err != nil {
		return nil, err
	}

	return block, nil
}

// fetchLatestBlockNumber fetches the latest block number from a provider
func fetchLatestBlockNumber(ctx context.Context, provider Provider) (uint64, error) {
	var blockNumber uint64
	operation := func() error {
		url := fmt.Sprintf(provider.URL, provider.ProjectID)
		req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
		if err != nil {
			return fmt.Errorf("error creating request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error fetching block number: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		var result struct {
			Result string `json:"result"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}

		blockNumber, err = hexToUint64(result.Result)
		if err != nil {
			return fmt.Errorf("error parsing block number: %w", err)
		}

		return nil
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 1 * time.Minute

	if err := backoff.Retry(operation, b); err != nil {
		return 0, err
	}

	return blockNumber, nil
}

// fetchBlocksConcurrently fetches blocks from all providers concurrently
func fetchBlocksConcurrently(ctx context.Context, db *sql.DB) {
	var wg sync.WaitGroup
	results := make(chan struct {
		Provider Provider
		Block    *Block
		Error    error
	}, len(providers))

	for _, provider := range providers {
		wg.Add(1)
		go func(p Provider) {
			defer wg.Done()
			block, err := fetchBlockFromProvider(ctx, p)
			results <- struct {
				Provider Provider
				Block    *Block
				Error    error
			}{p, block, err}
		}(provider)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var highestBlockNumber uint64
	for result := range results {
		if result.Error != nil {
			log.WithFields(logrus.Fields{
				"provider": result.Provider.Name,
				"error":    result.Error,
			}).Error("Failed to fetch block")
			providerErrorCounter.WithLabelValues(result.Provider.Name).Inc()
			continue
		}

		blockNumber, err := hexToUint64(result.Block.Number)
		if err != nil {
			log.WithFields(logrus.Fields{
				"provider": result.Provider.Name,
				"block":    result.Block.Number,
				"error":    err,
			}).Error("Failed to parse block number")
			continue
		}

		log.WithFields(logrus.Fields{
			"provider": result.Provider.Name,
			"block":    blockNumber,
		}).Info("Fetched block")

		providerBlockGauge.WithLabelValues(result.Provider.Name).Set(float64(blockNumber))
		if blockNumber > highestBlockNumber {
			highestBlockNumber = blockNumber
			latestBlockGauge.Set(float64(blockNumber))
		}

		if db != nil {
			if err := storeBlockInDB(db, result.Provider.Name, result.Block); err != nil {
				log.WithFields(logrus.Fields{
					"provider": result.Provider.Name,
					"block":    blockNumber,
					"error":    err,
				}).Error("Failed to store block in database")
			}
		}
	}

	// Update sync status
	networkLatestBlock, err := fetchLatestBlockNumber(ctx, providers[0]) // Using the first provider to get network status
	if err != nil {
		log.WithError(err).Error("Failed to fetch network latest block number")
	} else {
		updateSyncStatus(highestBlockNumber, networkLatestBlock)
	}
}

// updateSyncStatus updates the sync status and blocks behind metrics
func updateSyncStatus(ourLatestBlock, networkLatestBlock uint64) {
	if ourLatestBlock >= networkLatestBlock {
		syncStatusGauge.Set(1) // Synced
		blocksBehindGauge.Set(0)
	} else {
		syncStatusGauge.Set(0) // Syncing
		blocksBehindGauge.Set(float64(networkLatestBlock - ourLatestBlock))
	}
}

func hexToUint64(hexStr string) (uint64, error) {
	// Remove "0x" prefix if present
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return strconv.ParseUint(hexStr, 16, 64)
}

func storeBlockInDB(db *sql.DB, providerName string, block *Block) error {
	_, err := db.Exec(`
		INSERT INTO blocks (provider, block_number, block_hash, timestamp)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (provider, block_number) DO UPDATE
		SET block_hash = EXCLUDED.block_hash, timestamp = EXCLUDED.timestamp
	`, providerName, block.Number, block.Hash, block.Timestamp)
	return err
}

func main() {
	// Set initial log level from environment variable
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	if err := setLogLevel(logLevel); err != nil {
		log.Fatal("Invalid LOG_LEVEL:", err)
	}

	// Get port from environment variable, default to 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Connect to PostgreSQL
	db, err := connectDB()
	if err != nil {
		log.Fatal("Error connecting to PostgreSQL:", err)
	}
	if db != nil {
		defer db.Close()
	}

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a ticker to fetch blocks every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// Start the block fetching loop
	go func() {
		for {
			select {
			case <-ticker.C:
				fetchBlocksConcurrently(ctx, db)
			case <-ctx.Done():
				log.Info("Shutting down block fetcher")
				return
			}
		}
	}()

	// Set up HTTP server
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/healthz/live", livenessProbe)
	http.HandleFunc("/healthz/ready", readinessProbe(db))
	http.HandleFunc("/healthz/startup", startupProbe(5)) // Adjust threshold as needed
	http.HandleFunc("/log/", logLevelHandler)

	// Graceful shutdown
	srv := &http.Server{
		Addr: fmt.Sprintf(":%s", port),
	}

	go func() {
		log.Infof("HTTP server starting on port: %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()

	// Handle shutdown signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Info("Shutting down gracefully")
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("HTTP server Shutdown: %v", err)
	}
	log.Info("Server stopped")
}
