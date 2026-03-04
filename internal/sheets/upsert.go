package sheets

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	libSheets "github.com/Berry-rock-code/integration-hub/sheets"
	gsheets "google.golang.org/api/sheets/v4"
)

type Writer struct {
	Sheets *libSheets.Client

	SheetTitle string
	HeaderRow  int
	DataRow    int

	KeyHeader    string
	OwnedHeaders map[string]struct{}
}

func (w Writer) UpsertPreserving(ctx context.Context, inputHeaders []string, newRows [][]interface{}) error {
	if w.Sheets == nil {
		return fmt.Errorf("Writer: Sheets client is nil")
	}
	if w.SheetTitle == "" {
		return fmt.Errorf("Writer: SheetTitle required")
	}
	if w.HeaderRow <= 0 || w.DataRow <= 0 || w.DataRow <= w.HeaderRow {
		return fmt.Errorf("Writer: invalid HeaderRow/DataRow")
	}
	if strings.TrimSpace(w.KeyHeader) == "" {
		return fmt.Errorf("Writer: KeyHeader required")
	}
	if len(inputHeaders) == 0 {
		return fmt.Errorf("Writer: inputHeaders empty")
	}

	if err := w.Sheets.EnsureSheet(ctx, w.SheetTitle); err != nil {
		return err
	}

	sheetHeaders, numCols, err := w.readSheetHeaders(ctx)
	if err != nil {
		return err
	}
	if len(sheetHeaders) == 0 {
		return fmt.Errorf("Writer: header row %d is empty", w.HeaderRow)
	}

	keyIdx := w.findSheetIndex(sheetHeaders, w.KeyHeader)
	if keyIdx < 0 {
		keyIdx = findHeaderIndexAny(sheetHeaders, []string{"Account Number"})
	}
	if keyIdx < 0 {
		log.Printf("DEBUG KeyHeader=%q HeaderRow=%d HeaderCount=%d", w.KeyHeader, w.HeaderRow, len(sheetHeaders))
		log.Printf("DEBUG headers=%q", sheetHeaders)
		return fmt.Errorf(`Writer: key header %q not found in sheet header row`, w.KeyHeader)
	}

	readA1 := fmt.Sprintf("%s!A%d:%s", w.SheetTitle, w.DataRow, a1Col(numCols-1)+"50000")
	existing, err := w.Sheets.ReadRange(ctx, readA1)
	if err != nil {
		return err
	}

	existingByKey := make(map[string][]interface{}, len(existing))
	for _, r := range existing {
		norm := normalizeRowLen(r, numCols)
		if keyIdx >= len(norm) {
			continue
		}
		k := normalizeLeaseIDKey(keyString(norm[keyIdx]))
		if k == "" {
			continue
		}
		if _, ok := existingByKey[k]; !ok {
			existingByKey[k] = norm
		}
	}

	inputIdx := map[string]int{}
	for i, h := range inputHeaders {
		nh := normalizeHeader(h)
		if _, ok := inputIdx[nh]; !ok {
			inputIdx[nh] = i
		}
	}

	owned := map[string]struct{}{}
	for k := range w.OwnedHeaders {
		owned[k] = struct{}{}
	}
	owned[w.KeyHeader] = struct{}{}

	type colMap struct{ in, out int }
	mapping := map[string]colMap{}
	for canonical := range owned {
		in := -1
		if idx, ok := inputIdx[normalizeHeader(canonical)]; ok {
			in = idx
		}
		if in < 0 {
			in = findHeaderIndexAny(inputHeaders, ColumnAliases[canonical])
		}

		out := w.findSheetIndex(sheetHeaders, canonical)
		if out < 0 {
			return fmt.Errorf("Writer: sheet missing required column for %q", canonical)
		}
		mapping[canonical] = colMap{in: in, out: out}
	}

	// Merge rows into sheet-order slices of length numCols.
	merged := make([][]interface{}, 0, len(newRows))
	for _, r := range newRows {
		kIn := mapping[w.KeyHeader].in
		if kIn < 0 || kIn >= len(r) {
			continue
		}
		k := normalizeLeaseIDKey(keyString(r[kIn]))
		if k == "" {
			continue
		}

		var outRow []interface{}
		isExistingRow := false

		if ex, ok := existingByKey[k]; ok {
			isExistingRow = true
			outRow = make([]interface{}, numCols)
			copy(outRow, ex)
		} else {
			outRow = make([]interface{}, numCols)
		}

		for canonical, m := range mapping {
			if m.in < 0 || m.in >= len(r) {
				continue
			}

			// --- THE TWEAK: PRESERVE "DATE FIRST ADDED" ---
			if isExistingRow && strings.EqualFold(strings.TrimSpace(canonical), "Date First Added") { // <-- Updated here
				if m.out < len(outRow) && outRow[m.out] != nil {
					existingVal := strings.TrimSpace(fmt.Sprint(outRow[m.out]))
					if existingVal != "" {
						continue // Skips overwriting outRow[m.out]
					}
				}
			}
			// ----------------------------------------

			outRow[m.out] = r[m.in]
		}

		merged = append(merged, outRow)
	}

	// === Handle Updates and Appends Manually to fix the Row Placement ===
	var updateRanges []*gsheets.ValueRange
	var toAppend [][]interface{}

	// Find the absolute last row that has data in the Lease ID column
	maxExistingRow := w.DataRow - 1
	keyToRowNum := make(map[string]int)

	for i, r := range existing {
		sheetRow := w.DataRow + i
		if keyIdx < len(r) {
			k := normalizeLeaseIDKey(keyString(r[keyIdx]))
			if k != "" {
				if sheetRow > maxExistingRow {
					maxExistingRow = sheetRow
				}
				if _, ok := keyToRowNum[k]; !ok {
					keyToRowNum[k] = sheetRow
				}
			}
		}
	}

	// Route each merged row to either Update or Append
	for _, outRow := range merged {
		if keyIdx >= len(outRow) {
			continue
		}
		k := normalizeLeaseIDKey(keyString(outRow[keyIdx]))
		if rowNum, ok := keyToRowNum[k]; ok {
			// Update existing row
			a1 := fmt.Sprintf("%s!%s%d:%s%d", w.SheetTitle, a1Col(0), rowNum, a1Col(numCols-1), rowNum)
			updateRanges = append(updateRanges, &gsheets.ValueRange{
				Range:  a1,
				Values: [][]interface{}{outRow},
			})
		} else {
			// Append new row
			toAppend = append(toAppend, outRow)
		}
	}

	// 1. Send updates in a batch
	if len(updateRanges) > 0 {
		if err := BatchUpdateValues(ctx, w.Sheets.Service(), w.Sheets.SpreadsheetID, updateRanges); err != nil {
			return fmt.Errorf("UpsertRows batch update: %w", err)
		}
	}

	// 2. Explicitly write new rows directly under the last known Lease ID
	if len(toAppend) > 0 {
		startRow := maxExistingRow + 1
		endRow := startRow + len(toAppend) - 1
		appendA1 := fmt.Sprintf("%s!%s%d:%s%d", w.SheetTitle, a1Col(0), startRow, a1Col(numCols-1), endRow)

		if err := w.Sheets.WriteRange(ctx, appendA1, toAppend); err != nil {
			return fmt.Errorf("UpsertRows write appends: %w", err)
		}
	}

	return nil
}

func (w Writer) GetExistingKeyRows(ctx context.Context) (map[string]int, []string, error) {
	if w.Sheets == nil {
		return nil, nil, fmt.Errorf("Writer: Sheets client is nil")
	}
	if w.SheetTitle == "" {
		return nil, nil, fmt.Errorf("Writer: SheetTitle required")
	}
	if w.HeaderRow <= 0 || w.DataRow <= 0 || w.DataRow <= w.HeaderRow {
		return nil, nil, fmt.Errorf("Writer: invalid HeaderRow/DataRow")
	}
	if strings.TrimSpace(w.KeyHeader) == "" {
		return nil, nil, fmt.Errorf("Writer: KeyHeader required")
	}

	if err := w.Sheets.EnsureSheet(ctx, w.SheetTitle); err != nil {
		return nil, nil, err
	}

	headers, _, err := w.readSheetHeaders(ctx)
	if err != nil {
		return nil, nil, err
	}
	keyIdx := w.findSheetIndex(headers, w.KeyHeader)
	if keyIdx < 0 {
		return map[string]int{}, headers, nil
	}

	col := a1Col(keyIdx)
	readA1 := fmt.Sprintf("%s!%s%d:%s50000", w.SheetTitle, col, w.DataRow, col)
	vals, err := w.Sheets.ReadRange(ctx, readA1)
	if err != nil {
		return nil, nil, err
	}

	out := make(map[string]int, len(vals))
	for i, row := range vals {
		if len(row) == 0 {
			continue
		}
		k := normalizeLeaseIDKey(keyString(row[0]))
		if k == "" {
			continue
		}
		if _, ok := out[k]; !ok {
			out[k] = w.DataRow + i
		}
	}

	return out, headers, nil
}

func (w Writer) QuickUpdateBalances(ctx context.Context, keyToRow map[string]int, sheetHeaders []string, balances map[int]float64) (int, error) {
	if w.Sheets == nil {
		return 0, fmt.Errorf("Writer: Sheets client is nil")
	}
	if len(keyToRow) == 0 {
		return 0, nil
	}

	owedIdx := w.findSheetIndex(sheetHeaders, "Amount Owed:")
	if owedIdx < 0 {
		return 0, fmt.Errorf("Writer: sheet missing Amount Owed column")
	}
	dateIdx := w.findSheetIndex(sheetHeaders, "Last Edited Date")
	if dateIdx < 0 {
		return 0, fmt.Errorf("Writer: sheet missing Last Edited Date column")
	}

	today := time.Now().Format("01/02/2006")
	updates := make([]*gsheets.ValueRange, 0, len(keyToRow)*2)
	updated := 0

	for k, rowNum := range keyToRow {
		id, err := strconv.Atoi(normalizeLeaseIDKey(k))
		if err != nil || id <= 0 {
			continue
		}
		bal := balances[id]

		owedA1 := fmt.Sprintf("%s!%s%d", w.SheetTitle, a1Col(owedIdx), rowNum)
		dateA1 := fmt.Sprintf("%s!%s%d", w.SheetTitle, a1Col(dateIdx), rowNum)

		updates = append(updates,
			&gsheets.ValueRange{Range: owedA1, Values: [][]interface{}{{bal}}},
			&gsheets.ValueRange{Range: dateA1, Values: [][]interface{}{{today}}},
		)
		updated++
	}

	if err := BatchUpdateValues(ctx, w.Sheets.Service(), w.Sheets.SpreadsheetID, updates); err != nil {
		return 0, err
	}

	return updated, nil
}

func (w Writer) findSheetIndex(sheetHeaders []string, canonical string) int {
	if sheetHeaders == nil {
		return -1
	}

	canonical = strings.TrimSpace(canonical)

	if aliases, ok := ColumnAliases[canonical]; ok {
		return findHeaderIndexAny(sheetHeaders, aliases)
	}

	nc := normalizeHeader(canonical)
	for k, aliases := range ColumnAliases {
		if normalizeHeader(k) == nc {
			return findHeaderIndexAny(sheetHeaders, aliases)
		}
	}

	return findHeaderIndexAny(sheetHeaders, []string{canonical})
}

func (w Writer) readSheetHeaders(ctx context.Context) ([]string, int, error) {
	hA1 := fmt.Sprintf("%s!A%d:ZZ%d", w.SheetTitle, w.HeaderRow, w.HeaderRow)
	vals, err := w.Sheets.ReadRange(ctx, hA1)
	if err != nil {
		return nil, 0, err
	}
	if len(vals) == 0 || len(vals[0]) == 0 {
		return nil, 0, nil
	}

	raw := vals[0]
	headers := make([]string, len(raw))
	for i, cell := range raw {
		headers[i] = strings.TrimSpace(fmt.Sprint(cell))
	}

	last := -1
	for i := len(headers) - 1; i >= 0; i-- {
		if strings.TrimSpace(headers[i]) != "" {
			last = i
			break
		}
	}
	if last < 0 {
		return headers, 0, nil
	}

	return headers[:last+1], last + 1, nil
}

func normalizeLeaseIDKey(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if strings.Contains(s, ".") {
		s = strings.Split(s, ".")[0]
	}
	return strings.TrimSpace(s)
}
