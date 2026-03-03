package sheets

import (
	"fmt"
	"strings"
)

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
