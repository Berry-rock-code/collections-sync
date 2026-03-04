package build

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Berry-rock-code/integration-hub/buildium"
)

// ActiveOwedFetchConfig controls bulk mode behavior.
type ActiveOwedFetchConfig struct {
	MaxPages int
	MaxRows  int

	BalTimeout    time.Duration
	LeaseTimeout  time.Duration
	TenantTimeout time.Duration
	TenantSleep   time.Duration

	ExistingLeaseIDs map[int]bool
}

type DelinquentRow struct {
	LeaseID int

	Name    string
	Address string
	Phone   string
	Email   string

	AmountOwed float64
	DateAdded  string // <-- Here is the missing field!
}

func FetchActiveOwedRows(ctx context.Context, c *buildium.Client, cfg ActiveOwedFetchConfig) ([]DelinquentRow, int, error) {
	// Step A: balances
	bCtx, cancel := context.WithTimeout(ctx, cfg.BalTimeout)
	debtMap, err := c.FetchOutstandingBalances(bCtx)
	cancel()
	if err != nil {
		return nil, 0, fmt.Errorf("FetchOutstandingBalances: %w", err)
	}

	// Step B: leases
	lCtx, cancel := context.WithTimeout(ctx, cfg.LeaseTimeout)
	var leases []buildium.Lease
	if cfg.MaxPages == 0 {
		leases, err = c.ListActiveLeases(lCtx)
	} else {
		leases, err = c.ListActiveLeasesLimited(lCtx, cfg.MaxPages)
	}
	cancel()
	if err != nil {
		return nil, 0, fmt.Errorf("ListActiveLeases: %w", err)
	}

	log.Printf("Scanning %d total active leases concurrently...", len(leases))

	// Step C: Concurrent join + tenant detail lookups
	var out []DelinquentRow
	var outMutex sync.Mutex
	var tenantCache sync.Map

	tenantTimeout := cfg.TenantTimeout
	if tenantTimeout == 0 {
		tenantTimeout = 5 * time.Second
	}

	// --- CONCURRENCY SETUP ---
	maxWorkers := 8
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	todayDate := time.Now().Format("01/02/2006") // <-- Grab today's date once

	for _, lease := range leases {
		owed := debtMap[lease.ID]

		isExisting := false
		if cfg.ExistingLeaseIDs != nil {
			isExisting = cfg.ExistingLeaseIDs[lease.ID]
		}

		if owed <= 0 && !isExisting {
			continue
		}

		// Check if we hit MaxRows before launching a new worker
		outMutex.Lock()
		reachedMax := cfg.MaxRows > 0 && len(out) >= cfg.MaxRows
		outMutex.Unlock()
		if reachedMax {
			break
		}

		wg.Add(1)
		sem <- struct{}{} // Block here if 8 workers are already busy

		// Launch Goroutine
		go func(l buildium.Lease, amountOwed float64) {
			defer wg.Done()
			defer func() { <-sem }() // Free up the worker slot when finished

			tenantID := pickActiveTenantID(l)
			addr := leaseAddress(l, nil)

			if tenantID == 0 {
				appendSafeRow(&out, &outMutex, DelinquentRow{
					LeaseID:    l.ID,
					Name:       "(no active tenant found)",
					Address:    addr,
					AmountOwed: amountOwed,
					DateAdded:  todayDate, // <-- Injected here
				})
				return
			}

			// Check thread-safe cache
			var td buildium.TenantDetails
			if cached, ok := tenantCache.Load(tenantID); ok {
				td = cached.(buildium.TenantDetails)
			} else {
				// Not in cache, fetch from API
				tCtx, cancelT := context.WithTimeout(ctx, tenantTimeout)
				tdFetched, err := c.GetTenantDetails(tCtx, tenantID)
				cancelT()

				if err != nil {
					appendSafeRow(&out, &outMutex, DelinquentRow{
						LeaseID:    l.ID,
						Name:       "(tenant lookup failed)",
						Address:    addr,
						AmountOwed: amountOwed,
						DateAdded:  todayDate, // <-- Injected here
					})
					return
				}

				if cfg.TenantSleep > 0 {
					time.Sleep(cfg.TenantSleep)
				}

				td = tdFetched
				tenantCache.Store(tenantID, td)
			}

			addr = leaseAddress(l, &td)

			appendSafeRow(&out, &outMutex, DelinquentRow{
				LeaseID:    l.ID,
				Name:       strings.TrimSpace(td.FirstName + " " + td.LastName),
				Address:    addr,
				Phone:      firstPhone(td),
				Email:      td.Email,
				AmountOwed: amountOwed,
				DateAdded:  todayDate, // <-- Injected here
			})

		}(lease, owed)
	}

	wg.Wait()

	// biggest owed first
	sort.Slice(out, func(i, j int) bool { return out[i].AmountOwed > out[j].AmountOwed })

	if cfg.MaxRows > 0 && len(out) > cfg.MaxRows {
		out = out[:cfg.MaxRows]
	}

	return out, len(leases), nil
}

func appendSafeRow(out *[]DelinquentRow, mu *sync.Mutex, row DelinquentRow) {
	mu.Lock()
	defer mu.Unlock()
	*out = append(*out, row)
}

func pickActiveTenantID(lease buildium.Lease) int {
	for _, t := range lease.Tenants {
		if strings.EqualFold(t.Status, "Active") {
			return t.ID
		}
	}
	if len(lease.Tenants) > 0 {
		return lease.Tenants[0].ID
	}
	return 0
}

func leaseAddress(lease buildium.Lease, td *buildium.TenantDetails) string {
	if lease.Unit != nil && lease.Unit.Address != nil && lease.Unit.Address.AddressLine1 != "" {
		return lease.Unit.Address.AddressLine1
	}
	if td != nil && td.Address != nil && td.Address.AddressLine1 != "" {
		return td.Address.AddressLine1
	}
	return ""
}

func firstPhone(td buildium.TenantDetails) string {
	if len(td.PhoneNumbers) == 0 {
		return ""
	}
	return td.PhoneNumbers[0].Number
}
