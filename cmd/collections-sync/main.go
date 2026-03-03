package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Berry-rock-code/integration-hub/buildium"
	"github.com/Berry-rock-code/integration-hub/config"
	"github.com/Berry-rock-code/integration-hub/httpx"
	libSheets "github.com/Berry-rock-code/integration-hub/sheets"

	"github.com/Berry-rock-code/collections-sync/internal/app"
)

func main() {
	var (
		spreadsheetID = flag.String("spreadsheet-id", "", "Google Spreadsheet ID (required)")
		sheetTitle    = flag.String("sheet", "Collections", "Sheet tab title")
		maxPages      = flag.Int("max-pages", 5, "Max pages of leases to scan. Use 0 for no cap.")
		maxRows       = flag.Int("max-rows", 0, "Max delinquent rows to write. Use 0 for no cap.")
		timeout       = flag.Duration("timeout", 10*time.Minute, "Overall run timeout")

		balTimeout    = flag.Duration("balances-timeout", 120*time.Second, "Timeout for fetching outstanding balances")
		leaseTimeout  = flag.Duration("leases-timeout", 240*time.Second, "Timeout for fetching active leases")
		tenantTimeout = flag.Duration("tenant-timeout", 20*time.Second, "Timeout per tenant details request")
	)
	flag.Parse()

	config.LoadDotEnv()

	if strings.TrimSpace(*spreadsheetID) == "" {
		fmt.Fprintln(os.Stderr, "Missing --spreadsheet-id")
		os.Exit(2)
	}

	baseURL := mustEnv("BUILDIUM_BASE_URL")
	clientID := mustEnv("BUILDIUM_CLIENT_ID")
	clientSecret := mustEnv("BUILDIUM_CLIENT_SECRET")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	bClient := buildium.New(baseURL, clientID, clientSecret, httpx.NewDefaultClient())

	sClient, err := libSheets.NewClient(ctx, *spreadsheetID)
	if err != nil {
		fatal("Sheets client init failed", err)
	}

	cfg := app.Config{
		SheetTitle:    *sheetTitle,
		HeaderRow:     2,
		DataRow:       3,
		MaxPages:      *maxPages,
		MaxRows:       *maxRows,
		BalTimeout:    *balTimeout,
		LeaseTimeout:  *leaseTimeout,
		TenantTimeout: *tenantTimeout,
	}

	if err := app.Run(ctx, bClient, sClient, cfg); err != nil {
		fatal("Run failed", err)
	}

	fmt.Println("Done.")
}

func mustEnv(key string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		fmt.Fprintf(os.Stderr, "Missing required env var: %s\n", key)
		os.Exit(2)
	}
	return v
}

func fatal(msg string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", msg, err)
	os.Exit(2)
}
