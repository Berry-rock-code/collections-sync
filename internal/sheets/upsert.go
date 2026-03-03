package sheets

import (
	"context"
	"fmt"

	libSheets "github.com/Berry-rock-code/integration-hub/sheets"
)

type Writer struct {
	Sheets *libSheets.Client

	SheetTitle string
	HeaderRow  int
	DataRow    int

	KeyHeader string
	Headers   []string

	OwnedHeaders map[string]struct{}
}

func (w Writer) UpsertPreserving(ctx context.Context, newRows [][]interface{}) error {
	if w.Sheets == nil {
		return fmt.Errorf("Writer: Sheets client is nil")
	}
	if w.SheetTitle == "" {
		return fmt.Errorf("Writer: SheetTitle required")
	}
	if w.HeaderRow <= 0 || w.DataRow <= 0 || w.DataRow <= w.HeaderRow {
		return fmt.Errorf("Writer: invalid HeaderRow/DataRow")
	}
	if len(w.Headers) == 0 {
		return fmt.Errorf("Writer: Headers empty")
	}

	// Ensure tab exists
	if err := w.Sheets.EnsureSheet(ctx, w.SheetTitle); err != nil {
		return err
	}

	// Read existing header + data to preserve human columns
	// We'll let lib.UpsertRows write headers too, but we need the existing rows first to merge.
	// We read the full data width = len(headers).
	numCols := len(w.Headers)

	// Read existing data region (big enough)
	readA1 := fmt.Sprintf("%s!A%d:%s", w.SheetTitle, w.DataRow, a1Col(numCols)+"50000")
	existing, err := w.Sheets.ReadRange(ctx, readA1)
	if err != nil {
		return err
	}

	keyIdx := findHeaderIndex(w.Headers, w.KeyHeader)
	if keyIdx < 0 {
		return fmt.Errorf("Writer: key header %q not found", w.KeyHeader)
	}

	// Map key -> existing row values
	existingByKey := make(map[string][]interface{}, len(existing))
	for _, r := range existing {
		// Convert [][]interface{} row to []interface{} and normalize
		norm := normalizeRowLen(r, numCols)
		if keyIdx >= len(norm) {
			continue
		}
		k := keyString(norm[keyIdx])
		if k == "" {
			continue
		}
		if _, ok := existingByKey[k]; !ok {
			existingByKey[k] = norm
		}
	}

	// Merge
	owned := w.OwnedHeaders
	if owned == nil {
		owned = map[string]struct{}{}
	}

	merged := make([][]interface{}, 0, len(newRows))
	for _, r := range newRows {
		normNew := normalizeRowLen(r, numCols)
		if keyIdx >= len(normNew) {
			continue
		}
		k := keyString(normNew[keyIdx])
		if k == "" {
			continue
		}

		if ex, ok := existingByKey[k]; ok {
			// start from existing, overwrite owned cols
			out := make([]interface{}, numCols)
			copy(out, ex)

			// Keep track of which headers we have already updated to avoid writing duplicate columns (like Phone Number) twice.
			updatedOwned := make(map[string]bool)
			for i, h := range w.Headers {
				if _, isOwned := owned[h]; isOwned {
					// Only update the first occurrence of an owned header
					if !updatedOwned[h] {
						out[i] = normNew[i]
						updatedOwned[h] = true
					}
				}
			}
			merged = append(merged, out)
		} else {
			// new row
			merged = append(merged, normNew)
		}
	}

	// Delegate to library upsert (keyed on Lease ID)
	return w.Sheets.UpsertRows(ctx, libSheets.UpsertOptions{
		SheetTitle:    w.SheetTitle,
		HeaderRow:     w.HeaderRow,
		DataRow:       w.DataRow,
		KeyHeader:     w.KeyHeader,
		EnsureHeaders: true,
		Headers:       w.Headers,
		NumColumns:    numCols,
	}, merged)
}

// GetExistingKeys reads the sheet and returns a map of existing Lease IDs.
func (w Writer) GetExistingKeys(ctx context.Context) (map[string]bool, error) {
	if w.Sheets == nil {
		return nil, fmt.Errorf("Writer: Sheets client is nil")
	}
	if w.SheetTitle == "" {
		return nil, fmt.Errorf("Writer: SheetTitle required")
	}
	if err := w.Sheets.EnsureSheet(ctx, w.SheetTitle); err != nil {
		return nil, err
	}

	numCols := len(w.Headers)
	readA1 := fmt.Sprintf("%s!A%d:%s", w.SheetTitle, w.DataRow, a1Col(numCols)+"50000")
	existing, err := w.Sheets.ReadRange(ctx, readA1)
	if err != nil {
		return nil, err
	}

	keyIdx := findHeaderIndex(w.Headers, w.KeyHeader)
	if keyIdx < 0 {
		// Just in case it wasn't created yet or we can't find it, we'll return empty map safely
		return map[string]bool{}, nil
	}

	keys := make(map[string]bool)
	for _, r := range existing {
		norm := normalizeRowLen(r, numCols)
		if keyIdx >= len(norm) {
			continue
		}
		k := keyString(norm[keyIdx])
		if k == "" {
			continue
		}
		keys[k] = true
	}
	return keys, nil
}

// a1Col returns column letters for 1-based col number.
func a1Col(col int) string {
	if col <= 0 {
		return "A"
	}
	s := ""
	for col > 0 {
		col--
		s = string(rune('A'+(col%26))) + s
		col /= 26
	}
	return s
}
