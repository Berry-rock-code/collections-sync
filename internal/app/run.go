package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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

	// Mode: "bulk" or "quick".
	Mode string

	MaxPages int
	MaxRows  int

	BalTimeout    time.Duration
	LeaseTimeout  time.Duration
	TenantTimeout time.Duration
	TenantSleep   time.Duration
}

type Result struct {
	Mode          string
	ExistingKeys  int
	RowsPrepared  int
	RowsUpdated   int
	RowsAppended  int
	LeasesScanned int
}

func Run(ctx context.Context, b *buildium.Client, sh *libSheets.Client, cfg Config) (Result, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = "bulk"
	}
	if mode != "bulk" && mode != "quick" {
		return Result{}, fmt.Errorf("invalid mode %q (use bulk|quick)", cfg.Mode)
	}

	w := sheets.Writer{
		Sheets:       sh,
		SheetTitle:   cfg.SheetTitle,
		HeaderRow:    cfg.HeaderRow,
		DataRow:      cfg.DataRow,
		KeyHeader:    transform.KeyHeader(),
		OwnedHeaders: transform.OwnedHeaders(),
	}

	// Step 0: read existing Lease IDs in the sheet (and their row numbers)
	keyToRow, sheetHeaders, err := w.GetExistingKeyRows(ctx)
	if err != nil {
		return Result{}, err
	}

	// Convert to ints for Buildium.
	existingLeaseIDs := make(map[int]bool, len(keyToRow))
	leaseIDs := make([]int, 0, len(keyToRow))
	for k := range keyToRow {
		id, err := strconv.Atoi(strings.Split(k, ".")[0])
		if err == nil && id > 0 {
			existingLeaseIDs[id] = true
			leaseIDs = append(leaseIDs, id)
		}
	}

	res := Result{Mode: mode, ExistingKeys: len(existingLeaseIDs)}

	if mode == "quick" {
		fmt.Printf("Quick mode: updating balances for %d existing rows...\n", len(existingLeaseIDs))

		bCtx, cancel := context.WithTimeout(ctx, cfg.BalTimeout)
		debtMap, err := b.FetchOutstandingBalancesForLeaseIDs(bCtx, leaseIDs)
		cancel()
		if err != nil {
			return Result{}, fmt.Errorf("FetchOutstandingBalancesForLeaseIDs: %w", err)
		}

		n, err := w.QuickUpdateBalances(ctx, keyToRow, sheetHeaders, debtMap)
		if err != nil {
			return Result{}, err
		}
		res.RowsUpdated = n
		return res, nil
	}

	// BULK MODE
	fetchCfg := build.FetchConfig{
		MaxPages:         cfg.MaxPages,
		MaxRows:          cfg.MaxRows,
		BalTimeout:       cfg.BalTimeout,
		LeaseTimeout:     cfg.LeaseTimeout,
		TenantTimeout:    cfg.TenantTimeout,
		TenantSleep:      cfg.TenantSleep,
		ExistingLeaseIDs: existingLeaseIDs,
	}

	rows, leasesScanned, err := build.FetchDelinquentRows(ctx, b, fetchCfg)
	if err != nil {
		return Result{}, err
	}
	res.LeasesScanned = leasesScanned

	values := transform.ToSheetValues(rows)
	res.RowsPrepared = len(values)

	if err := w.UpsertPreserving(ctx, transform.Headers(), values); err != nil {
		return Result{}, err
	}

	// NOTE: integration-hub UpsertRows doesn't report counts. If you need exact
	// updated/added counts, we can compute it here, but it's optional.
	res.RowsUpdated = 0
	res.RowsAppended = 0
	return res, nil
}
