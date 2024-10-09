package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

// Struct to hold block data
type Block struct {
	Number       string        `json:"number"`
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

// Prometheus Metrics
var (
	httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "infura_http_request_duration_seconds",
		Help:    "Duration of HTTP requests to Infura",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "endpoint", "status"})

	dbRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "postgres_request_duration_seconds",
		Help:    "Duration of PostgreSQL requests",
		Buckets: prometheus.DefBuckets,
	}, []string{"query", "status"})
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(dbRequestDuration)
}

// Logger setup
var log = logrus.New()

// Connect to PostgreSQL with environment variables (Optional)
func connectDB() (*sql.DB, error) {
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbHost := os.Getenv("DB_HOST")

	// Optional DB setup: If any of the environment variables are missing, skip DB setup.
	if dbUser == "" || dbPassword == "" || dbName == "" || dbHost == "" {
		log.Warn("Database environment variables missing; skipping database connection")
		return nil, nil
	}

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, dbUser, dbPassword, dbName)
	return sql.Open("postgres", connStr)
}

// Fetches the latest finalized block (mock implementation, replace with actual logic)
func fetchLatestFinalizedBlock(ctx context.Context, infuraProjectID string) (int64, error) {
	// Implement the actual logic to fetch the latest finalized block
	// Here, we'll just return a mock value for demonstration purposes
	return 12345678, nil // Replace with actual fetching logic
}

// Parses a hexadecimal string to an int64
func parseHexString(hexStr string) (int64, error) {
	var num int64
	_, err := fmt.Sscanf(hexStr, "0x%x", &num)
	return num, err
}

// Fetches block data from Infura for a given block number using native HTTP request
func fetchBlock(ctx context.Context, infuraProjectID string, blockNumber int64) (*Block, error) {
	start := time.Now()

	// Infura API URL
	url := fmt.Sprintf("https://mainnet.infura.io/v3/%s", infuraProjectID)

	// Prepare the JSON-RPC payload as a string and wrap it in bytes.Buffer
	body := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x", true],"id":1}`, blockNumber)
	reqBody := bytes.NewBuffer([]byte(body))

	// Create new HTTP POST request with JSON payload
	req, err := http.NewRequest("POST", url, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// HTTP client to make the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Decode the JSON response into a Block struct
	var result struct {
		Result Block `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	// Calculate request duration and log it
	duration := time.Since(start).Seconds()
	log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"duration":     duration,
	}).Info("Block fetched successfully")

	// Prometheus metric for HTTP request duration
	httpRequestDuration.WithLabelValues("POST", "eth_getBlockByNumber", "success").Observe(duration)

	return &result.Result, nil
}

// Save blocks and transactions into PostgreSQL (if connected)
func saveBlockAndTransactions(db *sql.DB, block *Block) error {
	// If DB is nil, skip saving
	if db == nil {
		log.Warn("Skipping saving blocks and transactions due to no database connection")
		return nil
	}

	start := time.Now()

	// Save block
	timestamp, _ := time.Parse("2006-01-02T15:04:05Z", block.Timestamp)
	_, err := db.Exec("INSERT INTO blocks (block_number, timestamp) VALUES ($1, $2) ON CONFLICT DO NOTHING", block.Number, timestamp)
	if err != nil {
		dbRequestDuration.WithLabelValues("insert_block", "error").Observe(time.Since(start).Seconds())
		log.WithFields(logrus.Fields{
			"block_number": block.Number,
			"error":        err,
		}).Error("Failed to save block")
		return err
	}

	// Save transactions
	for _, tx := range block.Transactions {
		_, err := db.Exec("INSERT INTO transactions (tx_hash, from_address, to_address, block_number, value) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
			tx.Hash, tx.From, tx.To, block.Number, tx.Value)
		if err != nil {
			dbRequestDuration.WithLabelValues("insert_transaction", "error").Observe(time.Since(start).Seconds())
			log.WithFields(logrus.Fields{
				"tx_hash": tx.Hash,
				"error":   err,
			}).Error("Failed to save transaction")
			return err
		}
	}

	dbRequestDuration.WithLabelValues("insert_block_and_transactions", "success").Observe(time.Since(start).Seconds())
	log.WithFields(logrus.Fields{
		"block_number": block.Number,
		"tx_count":     len(block.Transactions),
	}).Info("Block and transactions saved successfully")
	return nil
}

// Asynchronously fetches blocks in batches and saves to DB
func batchFetchBlocks(ctx context.Context, infuraProjectID string, db *sql.DB, startBlock int64, endBlock int64) {
	var wg sync.WaitGroup
	blockChan := make(chan *Block, endBlock-startBlock+1)

	// Worker goroutine to save blocks to the DB
	go func() {
		for block := range blockChan {
			err := saveBlockAndTransactions(db, block)
			if err != nil {
				log.Println("Error saving block:", err)
			}
		}
	}()

	// Fetch blocks asynchronously
	for i := startBlock; i <= endBlock; i++ {
		wg.Add(1)
		go func(blockNumber int64) {
			defer wg.Done()
			block, err := fetchBlock(ctx, infuraProjectID, blockNumber)
			if err != nil {
				log.Printf("Error fetching block %d: %s", blockNumber, err)
				return
			}
			blockChan <- block
		}(i)
	}

	// Wait for all block fetches to finish
	wg.Wait()
	close(blockChan)
}

// Health check for liveness
func livenessProbe(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Liveness Probe: OK")
}

// Health check for readiness
func readinessProbe(w http.ResponseWriter, r *http.Request) {
	// Check if the database has current information
	var savedBlocksCount int
	err := db.QueryRow("SELECT COUNT(*) FROM blocks").Scan(&savedBlocksCount)
	if err != nil {
		http.Error(w, "Error checking saved blocks count", http.StatusInternalServerError)
		return
	}

	if savedBlocksCount == 0 {
		http.Error(w, "Not ready, no blocks saved", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Readiness Probe: OK")
}

// Health check for startup
func startupProbe(w http.ResponseWriter, r *http.Request) {
	// Check if the sync with Infura is complete
	// Here, implement logic to check if the sync is complete
	// For this example, we'll just return OK
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Startup Probe: OK")
}

func main() {
	// Set up structured logging with Logrus
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.InfoLevel)

	// Get Infura project ID from environment variable
	infuraProjectID := os.Getenv("INFURA_PROJECT_ID")
	if infuraProjectID == "" {
		log.Fatal("INFURA_PROJECT_ID environment variable is required")
	}

	// Connect to PostgreSQL (Optional)
	db, err := connectDB()
	if err != nil {
		log.Fatal("Error connecting to PostgreSQL:", err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	// Check for last saved block
	var lastBlock Block
	var startBlock int64

	err = db.QueryRow("SELECT block_number FROM blocks ORDER BY block_number DESC LIMIT 1").Scan(&lastBlock.Number)
	if err != nil {
		if err == sql.ErrNoRows {
			// No blocks saved, fetch the latest finalized block
			log.Info("No blocks saved, fetching the latest finalized block")
			latestBlock, err := fetchLatestFinalizedBlock(context.Background(), infuraProjectID)
			if err != nil {
				log.Fatal("Error fetching the latest finalized block:", err)
			}
			startBlock = latestBlock
		} else {
			log.Fatal("Error fetching last saved block number:", err)
		}
	} else {
		// Correctly handle the return values from parseHexString
		var err error
		startBlock, err = parseHexString(lastBlock.Number)
		if err != nil {
			log.Fatal("Error parsing last saved block number:", err)
		}
	}

	// Set the block range
	endBlock := startBlock + 100 // Adjust range as needed

	// Create a context to manage cancellations
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		cancel() // Cancel the context on shutdown
	}()

	// Start batch fetching
	batchFetchBlocks(ctx, infuraProjectID, db, startBlock, endBlock)

	// Start Prometheus HTTP metrics server
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/liveness", livenessProbe)
	http.HandleFunc("/readiness", readinessProbe)
	http.HandleFunc("/startup", startupProbe)

	log.Fatal(http.ListenAndServe(":2112", nil))
}
