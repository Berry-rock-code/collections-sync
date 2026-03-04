package build

import (
	"context"
	"fmt"
	"time"

	"github.com/Berry-rock-code/integration-hub/buildium"
)

// ActiveOwedFetchConfig controls bulk mode behavior.
type ActiveOwedFetchConfig struct {
	// Paging / caps
	MaxPages int
	MaxRows  int

	// Timeouts
	BalTimeout   time.Duration
	LeaseTimeout time.Duration

	// ExistingLeaseIDs lets us know which ones already exist in the sheet.
	ExistingLeaseIDs map[int]bool
}

// FetchActiveOwedRows returns rows for ALL active leases whose outstanding balance > 0.
// It returns:
// - rows: only leases with balance > 0 (both existing + new)
// - leasesScanned: number of active leases enumerated
func FetchActiveOwedRows(ctx context.Context, b *buildium.Client, cfg ActiveOwedFetchConfig) ([]Row, int, error) {
	// 1) Get balances (single scan) — fastest way to know who owes anything.
	balCtx, balCancel := context.WithTimeout(ctx, cfg.BalTimeout)
	debtMap, err := b.FetchOutstandingBalances(balCtx)
	balCancel()
	if err != nil {
		return nil, 0, fmt.Errorf("FetchOutstandingBalances: %w", err)
	}

	// 2) List active leases.
	leaseCtx, leaseCancel := context.WithTimeout(ctx, cfg.LeaseTimeout)
	leases, err := ListActiveLeases(ctx, b, cfg.MaxPages)
	leaseCancel()
	if err != nil {
		return nil, 0, err
	}

	leasesScanned := len(leases)
	rows := make([]Row, 0, 256)

	// 3) Emit a row for every active lease with balance > 0.
	for _, l := range leases {
		leaseID := l.LeaseID // adjust if your lease struct uses a different field name
		if leaseID <= 0 {
			continue
		}

		owed := debtMap[leaseID]
		if owed <= 0 {
			continue
		}

		// Build the row. Keep it consistent with your existing transform.ToSheetValues().
		// If your Row type includes more fields, fill what you can here.
		r := Row{
			LeaseID:      leaseID,
			AmountOwed:   owed,
			IsExpired:    l.IsExpired, // adjust field names
			Name:         l.TenantName,
			Address:      l.PropertyAddress,
			PhoneNumber:  l.Phone,
			Email:        l.Email,
			Market:       l.Market,
			DateAdded:    time.Now(), // or leave zero; transform can fill
			LastEditedAt: time.Now(),
		}

		rows = append(rows, r)

		if cfg.MaxRows > 0 && len(rows) >= cfg.MaxRows {
			break
		}
	}

	return rows, leasesScanned, nil
}
