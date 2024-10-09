package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

	infuraErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "infura_errors_total",
		Help: "Total number of Infura errors",
	})

	latestBlockGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "infura_latest_block",
		Help: "Latest block fetched from Infura",
	})

	finalizedBlockGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "infura_finalized_block",
		Help: "Latest finalized block from Infura",
	})
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(httpRequestDuration, infuraErrorCounter, latestBlockGauge, finalizedBlockGauge)
}

// Logger setup
var log = logrus.New()

// Set log level dynamically
func setLogLevel(level string) error {
	switch strings.ToLower(level) {
	case "info":
		log.SetLevel(logrus.InfoLevel)
	case "warning":
		log.SetLevel(logrus.WarnLevel)
	case "error":
		log.SetLevel(logrus.ErrorLevel)
	case "debug":
		log.SetLevel(logrus.DebugLevel)
	case "trace":
		log.SetLevel(logrus.TraceLevel)
	case "fatal":
		log.SetLevel(logrus.FatalLevel)
	case "panic":
		log.SetLevel(logrus.PanicLevel)
	default:
		return fmt.Errorf("invalid log level: %s", level)
	}
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

// Connect to PostgreSQL (optional for development mode)
func connectDB() (*sql.DB, error) {
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbHost := os.Getenv("DB_HOST")
	devMode := os.Getenv("DEV_MODE")

	// Optional DB setup: skip DB if in development mode
	if devMode == "true" {
		log.Warn("DEV_MODE enabled: Skipping database connection")
		return nil, nil
	}

	if dbUser == "" || dbPassword == "" || dbName == "" || dbHost == "" {
		log.Warn("Database environment variables missing; skipping database connection")
		return nil, nil
	}

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, dbUser, dbPassword, dbName)
	return sql.Open("postgres", connStr)
}

// Simulated probes (liveness, readiness, and startup) - implement these as needed
func livenessProbe() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Liveness Probe: OK")
	}
}

func readinessProbe(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			http.Error(w, "Database not connected", http.StatusServiceUnavailable)
			return
		}
		var result string
		err := db.QueryRow("EXPLAIN SELECT 1").Scan(&result)
		if err != nil {
			http.Error(w, "Database not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "Readiness Probe: OK")
	}
}

func startupProbe() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Startup Probe: OK")
	}
}

func main() {
	// Set up structured logging with Logrus
	log.SetFormatter(&logrus.JSONFormatter{})

	// Set initial log level from environment variable
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	if err := setLogLevel(logLevel); err != nil {
		log.Fatal("Invalid LOG_LEVEL:", err)
	}

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

	// Start Prometheus HTTP metrics server
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/liveness", livenessProbe())
	http.HandleFunc("/readiness", readinessProbe(db))
	http.HandleFunc("/startup", startupProbe())

	// Endpoint to change log level dynamically
	http.HandleFunc("/log/", logLevelHandler)

	// Graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		log.Info("Shutting down...")
		os.Exit(0)
	}()

	log.Fatal(http.ListenAndServe(":9000", nil))
}
