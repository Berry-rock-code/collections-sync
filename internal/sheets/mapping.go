package sheets

import (
	"fmt"
	"strings"
)

// a1Col converts a 0-based column index into an A1 column name.
// 0 -> A, 25 -> Z, 26 -> AA, ...
func a1Col(colIdx0 int) string {
	col := colIdx0 + 1
	if col <= 0 {
		return "A"
	}
	out := ""
	for col > 0 {
		col--
		out = string(rune('A'+(col%26))) + out
		col /= 26
	}
	return out
}

func findHeaderIndex(headers []string, want string) int {
	want = strings.TrimSpace(strings.ToLower(want))
	for i, h := range headers {
		if strings.TrimSpace(strings.ToLower(h)) == want {
			return i
		}
	}
	return -1
}

func normalizeRowLen(row []interface{}, n int) []interface{} {
	out := make([]interface{}, n)
	for i := 0; i < n && i < len(row); i++ {
		out[i] = row[i]
	}
	return out
}

func keyString(v interface{}) string {
	return strings.TrimSpace(fmt.Sprintf("%v", v))
}
