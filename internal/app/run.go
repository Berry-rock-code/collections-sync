package app

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Berry-rock-code/integration-hub/buildium"
	libSheets "github.com/Berry-rock-code/integration-hub/sheets"

	"github.com/Berry-rock-code/collections-sync/internal/build"
	"github.com/Berry-rock-code/collections-sync/internal/sheets"
	"github.com/Berry-rock-code/collections-sync/internal/transform"
)

type Config struct {
	SheetTitle string
	HeaderRow  int
	DataRow    int

	MaxPages int
	MaxRows  int

	BalTimeout    time.Duration
	LeaseTimeout  time.Duration
	TenantTimeout time.Duration
}

func Run(ctx context.Context, b *buildium.Client, sh *libSheets.Client, cfg Config) error {
	fmt.Println("Step 0/4: Read existing Lease IDs from Google Sheet...")
	w := sheets.Writer{
		Sheets:       sh,
		SheetTitle:   cfg.SheetTitle,
		HeaderRow:    cfg.HeaderRow,
		DataRow:      cfg.DataRow,
		KeyHeader:    transform.KeyHeader(),
		Headers:      transform.Headers(),
		OwnedHeaders: transform.OwnedHeaders(),
	}
	existingKeysMap, err := w.GetExistingKeys(ctx)
	if err != nil {
		fmt.Printf("Warning: Failed to read existing keys (might be empty sheet): %v\n", err)
		existingKeysMap = make(map[string]bool)
	}
	existingLeaseIDs := make(map[int]bool)
	for k := range existingKeysMap {
		if id, err := strconv.Atoi(k); err == nil {
			existingLeaseIDs[id] = true
		}
	}
	fmt.Printf("Found %d existing leases in sheet.\n\n", len(existingLeaseIDs))

	fmt.Println("Step 1/4: Fetch Buildium delinquency data (balances + leases + tenant contact)...")

	fetchCfg := build.FetchConfig{
		MaxPages:         cfg.MaxPages,
		MaxRows:          cfg.MaxRows,
		BalTimeout:       cfg.BalTimeout,
		LeaseTimeout:     cfg.LeaseTimeout,
		TenantTimeout:    cfg.TenantTimeout,
		ExistingLeaseIDs: existingLeaseIDs,
	}

	rows, err := build.FetchDelinquentRows(ctx, b, fetchCfg)
	if err != nil {
		return err
	}
	fmt.Printf("Fetched delinquent rows (pre-write): %d\n\n", len(rows))

	fmt.Println("Step 2/4: Build sheet rows (matching header order)...")
	values := transform.ToSheetValues(rows)
	fmt.Printf("Prepared value rows: %d\n\n", len(values))

	fmt.Println("Step 3/4: Upsert into Google Sheet (preserve human columns)...")
	w = sheets.Writer{
		Sheets:       sh,
		SheetTitle:   cfg.SheetTitle,
		HeaderRow:    cfg.HeaderRow,
		DataRow:      cfg.DataRow,
		KeyHeader:    transform.KeyHeader(),
		Headers:      transform.Headers(),
		OwnedHeaders: transform.OwnedHeaders(),
	}

	if err := w.UpsertPreserving(ctx, values); err != nil {
		return err
	}

	fmt.Println("Step 4/4: Complete")
	return nil
}
