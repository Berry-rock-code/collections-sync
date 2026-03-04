package sheets

import "strings"

// ColumnAliases maps a canonical column name to acceptable header variants.
// This is how we avoid hardcoding column letters and stay compatible with
// slightly different sheet header names.
var ColumnAliases = map[string][]string{
	"Name":             {"Name", "Tenant Name"},
	"Address:":         {"Address:", "Address"},
	"Phone Number":     {"Phone Number", "Phone"},
	"Email":            {"Email", "Email Address"},
	"Amount Owed:":     {"Amount Owed:", "Amount Owed", "Balance"},
	"Lease ID":         {"Lease ID", "Account Number"},
	"Last Edited Date": {"Last Edited Date", "Date"},
	"Date First Added": {"Date First Added"},
}

func normalizeHeader(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func findHeaderIndexAny(headers []string, candidates []string) int {
	if len(headers) == 0 || len(candidates) == 0 {
		return -1
	}

	normHeaders := make([]string, len(headers))
	for i, h := range headers {
		normHeaders[i] = normalizeHeader(h)
	}

	for _, cand := range candidates {
		want := normalizeHeader(cand)
		if want == "" {
			continue
		}
		for i, h := range normHeaders {
			if h == want {
				return i
			}
		}
	}

	return -1
}
