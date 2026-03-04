package sheets

import (
	"context"
	"fmt"
	"time"

	gsheets "google.golang.org/api/sheets/v4"
)

// BatchUpdateValues performs a Sheets Values.BatchUpdate with chunking.
// This is used for "quick mode" to update only a couple cells per row.
func BatchUpdateValues(ctx context.Context, svc *gsheets.Service, spreadsheetID string, updates []*gsheets.ValueRange) error {
	if svc == nil {
		return fmt.Errorf("BatchUpdateValues: sheets service is nil")
	}
	if spreadsheetID == "" {
		return fmt.Errorf("BatchUpdateValues: spreadsheetID required")
	}
	if len(updates) == 0 {
		return nil
	}

	const chunkSize = 200
	for i := 0; i < len(updates); i += chunkSize {
		end := i + chunkSize
		if end > len(updates) {
			end = len(updates)
		}

		req := &gsheets.BatchUpdateValuesRequest{
			ValueInputOption: "USER_ENTERED",
			Data:             updates[i:end],
		}

		_, err := svc.Spreadsheets.Values.BatchUpdate(spreadsheetID, req).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("BatchUpdateValues: %w", err)
		}

		// Small pause to be gentle on Sheets quota.
		time.Sleep(150 * time.Millisecond)
	}

	return nil
}
