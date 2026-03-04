package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Berry-rock-code/integration-hub/buildium"
	libSheets "github.com/Berry-rock-code/integration-hub/sheets"

	"github.com/Berry-rock-code/collections-sync/internal/app"
)

type request struct {
	Mode     string `json:"mode"`
	MaxPages int    `json:"max_pages"`
	MaxRows  int    `json:"max_rows"`
}

func main() {
	// ---- Required env ----
	sheetID := mustEnvOneOf("SHEET_ID", "SPREADSHEET_ID")
	tab := mustEnvOneOf("WORKSHEET_NAME", "SHEET_TITLE")

	buildiumKey := mustEnv("BUILDIUM_KEY")
	buildiumSecret := mustEnv("BUILDIUM_SECRET")

	// ---- Optional env ----
	headerRow := envInt("HEADER_ROW", 1)
	dataRow := envInt("DATA_ROW", 2)

	port := env("PORT", "8080")
	apiURL := env("BUILDIUM_API_URL", "https://api.buildium.com/v1")

	// ---- Clients ----
	rootCtx := context.Background()

	httpClient := &http.Client{
		Timeout: 60 * time.Second,
	}

	bClient := buildium.New(apiURL, buildiumKey, buildiumSecret, httpClient)

	sClient, err := libSheets.NewClient(rootCtx, sheetID)
	if err != nil {
		log.Fatalf("failed to create sheets client: %v", err)
	}

	log.Printf(
		"collections-sync listening on :%s (sheet=%s tab=%s headerRow=%d dataRow=%d)",
		port, sheetID, tab, headerRow, dataRow,
	)

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}

		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		// Per-request context with a generous overall timeout
		// (bulk may take a while; you can tune this later)
		rCtx, cancel := context.WithTimeout(r.Context(), 25*time.Minute)
		defer cancel()

		cfg := app.Config{
			SheetTitle: tab,
			HeaderRow:  headerRow,
			DataRow:    dataRow,

			Mode: req.Mode,

			MaxPages: req.MaxPages,
			MaxRows:  req.MaxRows,

			// These timeouts are per-step inside the run:
			BalTimeout:    60 * time.Second,
			LeaseTimeout:  60 * time.Second,
			TenantTimeout: 60 * time.Second,

			// Slow down tenant calls a bit to be kind to Buildium
			TenantSleep: 250 * time.Millisecond,
		}

		result, err := app.Run(rCtx, bClient, sClient, cfg)
		if err != nil {
			log.Printf("run failed: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

func env(name, def string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	return v
}

func mustEnv(name string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		log.Fatalf("missing required env var: %s", name)
	}
	return v
}

func mustEnvOneOf(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	log.Fatalf("missing required env var (one of): %s", strings.Join(names, ", "))
	return ""
}

func envInt(name string, def int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
