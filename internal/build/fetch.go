package build

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Berry-rock-code/integration-hub/buildium"
)

type FetchConfig struct {
	MaxPages int
	MaxRows  int

	BalTimeout    time.Duration
	LeaseTimeout  time.Duration
	TenantTimeout time.Duration
}

type DelinquentRow struct {
	LeaseID int

	Name    string
	Address string
	Phone   string
	Email   string

	AmountOwed float64
}

func FetchDelinquentRows(ctx context.Context, c *buildium.Client, cfg FetchConfig) ([]DelinquentRow, error) {
	// Step A: balances
	bCtx, cancel := context.WithTimeout(ctx, cfg.BalTimeout)
	debtMap, err := c.FetchOutstandingBalances(bCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("FetchOutstandingBalances: %w", err)
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
		return nil, fmt.Errorf("ListActiveLeases: %w", err)
	}

	// Step C: join + tenant detail lookups (only for owed leases)
	tenantCache := make(map[int]buildium.TenantDetails)
	var out []DelinquentRow

	for _, lease := range leases {
		owed := debtMap[lease.ID]
		if owed <= 0 {
			continue
		}

		tenantID := pickActiveTenantID(lease)

		// Basic address even without tenant
		addr := leaseAddress(lease, nil)

		if tenantID == 0 {
			out = append(out, DelinquentRow{
				LeaseID:    lease.ID,
				Name:       "(no active tenant found)",
				Address:    addr,
				AmountOwed: owed,
				Phone:      "",
				Email:      "",
			})
			if cfg.MaxRows > 0 && len(out) >= cfg.MaxRows {
				break
			}
			continue
		}

		td, ok := tenantCache[tenantID]
		if !ok {
			tCtx, cancelT := context.WithTimeout(ctx, cfg.TenantTimeout)
			tdFetched, err := c.GetTenantDetails(tCtx, tenantID)
			cancelT()
			if err != nil {
				out = append(out, DelinquentRow{
					LeaseID:    lease.ID,
					Name:       "(tenant lookup failed)",
					Address:    addr,
					AmountOwed: owed,
				})
				if cfg.MaxRows > 0 && len(out) >= cfg.MaxRows {
					break
				}
				continue
			}
			td = tdFetched
			tenantCache[tenantID] = td
		}

		addr = leaseAddress(lease, &td)

		out = append(out, DelinquentRow{
			LeaseID:    lease.ID,
			Name:       strings.TrimSpace(td.FirstName + " " + td.LastName),
			Address:    addr,
			Phone:      firstPhone(td),
			Email:      td.Email,
			AmountOwed: owed,
		})

		if cfg.MaxRows > 0 && len(out) >= cfg.MaxRows {
			break
		}
	}

	// biggest owed first
	sort.Slice(out, func(i, j int) bool { return out[i].AmountOwed > out[j].AmountOwed })
	return out, nil
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
