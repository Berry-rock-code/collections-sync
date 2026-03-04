package transform

import (
	"strings"
	"time"

	"github.com/Berry-rock-code/collections-sync/internal/build"
)

func ToSheetValues(rows []build.DelinquentRow) [][]interface{} {
	headers := Headers()

	// Create a map of header names to their FIRST occurrence index
	headerIndices := make(map[string]int)
	for i, h := range headers {
		lowerH := strings.TrimSpace(strings.ToLower(h))
		if _, exists := headerIndices[lowerH]; !exists {
			headerIndices[lowerH] = i
		}
	}

	out := make([][]interface{}, 0, len(rows))
	// Match the legacy Python sheet format: MM/DD/YYYY
	now := time.Now().Format("01/02/2006")

	for _, r := range rows {
		row := make([]interface{}, len(headers))

		setValue := func(headerName string, value interface{}) {
			idx, ok := headerIndices[strings.TrimSpace(strings.ToLower(headerName))]
			if ok && idx < len(row) {
				row[idx] = value
			}
		}

		// Inject the new DateAdded variable from the struct
		setValue("Date Added", r.DateAdded)

		setValue("Name", r.Name)
		setValue("Address:", r.Address)
		setValue("Phone Number", r.Phone)
		setValue("Email", r.Email)
		setValue("Amount Owed:", r.AmountOwed)
		setValue("Last Edited Date", now)
		setValue("Lease ID", r.LeaseID)

		out = append(out, row)
	}

	return out
}
