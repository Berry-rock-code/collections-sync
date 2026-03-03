package transform

import (
	"time"

	"github.com/Berry-rock-code/collections-sync/internal/build"
)

func ToSheetValues(rows []build.DelinquentRow) [][]interface{} {
	headers := Headers()

	out := make([][]interface{}, 0, len(rows))
	now := time.Now().Format(time.RFC3339)

	for _, r := range rows {
		row := make([]interface{}, len(headers))

		// Map into known columns by position (matching Headers()).
		// 0 Name
		row[0] = r.Name
		// 1 Address:
		row[1] = r.Address
		// 2 Phone Number
		row[2] = r.Phone
		// 3 Email
		row[3] = r.Email
		// 4 Amount Owed:
		row[4] = r.AmountOwed

		// 13 Last Edited Date
		row[13] = now

		// 23 Lease ID
		row[23] = r.LeaseID

		// 24 Phone Number (duplicate column if your sheet has it)
		row[24] = r.Phone

		out = append(out, row)
	}

	return out
}
