package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Berry-rock-code/integration-hub/buildium"
	"github.com/Berry-rock-code/integration-hub/config"
	"github.com/Berry-rock-code/integration-hub/httpx"
	libSheets "github.com/Berry-rock-code/integration-hub/sheets"

	"github.com/Berry-rock-code/collections-sync/internal/app"
)

// Matches the legacy Python Cloud Run contract:
// request body: {"mode":"bulk"|"quick"}
type runRequest struct {
	Mode     string `json:"mode"`
	MaxPages int    `json:"max_pages"`
	MaxRows  int    `json:"max_rows"`
}

func main() {
	config.LoadDotEnv()

	// Env vars (prefer Python names, but accept integration-hub names too)
	sheetID := mustEnvFirst("SHEET_ID", "SPREADSHEET_ID")
	sheetTitle := envFirst("WORKSHEET_NAME", "SHEET_TITLE")
	if sheetTitle == "" {
		sheetTitle = "Collections"
	}

	baseURL := envFirst("BUILDIUM_API_URL", "BUILDIUM_BASE_URL")
	if baseURL == "" {
		baseURL = "https://api.buildium.com/v1"
	}
	clientID := mustEnvFirst("BUILDIUM_KEY", "BUILDIUM_CLIENT_ID")
	clientSecret := mustEnvFirst("BUILDIUM_SECRET", "BUILDIUM_CLIENT_SECRET")

	// Defaults (can be overridden by request JSON)
	defaultMaxPages := envInt("MAX_PAGES", 0) // 0 = full scan
	defaultMaxRows := envInt("MAX_ROWS", 0)

	// Timeouts (kept close to your Go CLI defaults)
	overallTimeout := envDuration("TIMEOUT", 10*time.Minute)
	balTimeout := envDuration("BALANCES_TIMEOUT", 120*time.Second)
	leaseTimeout := envDuration("LEASES_TIMEOUT", 240*time.Second)
	tenantTimeout := envDuration("TENANT_TIMEOUT", 20*time.Second)
	tenantSleep := envDuration("TENANT_SLEEP", 100*time.Millisecond)

	// Create shared clients once per container instance
	bClient := buildium.New(baseURL, clientID, clientSecret, httpx.NewDefaultClient())

	ctx, cancel := context.WithTimeout(context.Background(), overallTimeout)
	defer cancel()
	sClient, err := libSheets.NewClient(ctx, sheetID)
	if err != nil {
		log.Fatalf("sheets client init failed: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req runRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.MaxPages == 0 {
			req.MaxPages = defaultMaxPages
		}
		if req.MaxRows == 0 {
			req.MaxRows = defaultMaxRows
		}
		if strings.TrimSpace(req.Mode) == "" {
			req.Mode = "bulk"
		}

		rCtx, rCancel := context.WithTimeout(r.Context(), overallTimeout)
		defer rCancel()

		result, err := app.Run(rCtx, bClient, sClient, app.Config{
			SheetTitle:    sheetTitle,
			HeaderRow:     2,
			DataRow:       3,
			Mode:          req.Mode,
			MaxPages:      req.MaxPages,
			MaxRows:       req.MaxRows,
			BalTimeout:    balTimeout,
			LeaseTimeout:  leaseTimeout,
			TenantTimeout: tenantTimeout,
			TenantSleep:   tenantSleep,
		})
		if err != nil {
			log.Printf("run failed: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port
	log.Printf("collections-sync listening on %s (sheet=%s tab=%s)", addr, sheetID, sheetTitle)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func envFirst(keys ...string) string {
	for _, k := range keys {
		v := strings.TrimSpace(os.Getenv(k))
		if v != "" {
			return v
		}
	}
	return ""
}

func mustEnvFirst(keys ...string) string {
	v := envFirst(keys...)
	if v == "" {
		log.Fatalf("missing required env var (one of): %s", strings.Join(keys, ", "))
	}
	return v
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// Useful for debugging in Cloud Run logs.
func debugEnv(keys ...string) {
	for _, k := range keys {
		fmt.Printf("%s=%q\n", k, os.Getenv(k))
	}
}
