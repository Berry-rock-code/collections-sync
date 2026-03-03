package transform

func KeyHeader() string {
	return "Lease ID"
}

func Headers() []string {
	return []string{
		"Name",
		"Address:",
		"Phone Number",
		"Email",
		"Amount Owed:",
		"Date of 5 Day:",
		"Expired Lease",
		"Returned Payment",
		"Date of Next Payment",
		"Date of Last payment",
		"Payment Plan Details",
		"Missed Payment Plan and not Rescheduled",
		"Remarks:",
		"Last Edited Date",
		"Status",
		"CALL 1",
		"CALL 2",
		"CALL 3",
		"CALL 4",
		"CALL 5",
		"Last Call Date",
		"Eviction Filed Date",
		"Eviction Court Date",
		"Lease ID",
		"Phone Number", // appears twice in the list; kept for consistency with the actual sheet
		"Date Status Changed to Eviction",
	}
}

// OwnedHeaders are the columns this automation “owns” and will overwrite on upsert.
// Everything else is preserved if the Lease ID already exists in the sheet.
func OwnedHeaders() map[string]struct{} {
	return map[string]struct{}{
		"Name":             {},
		"Address:":         {},
		"Phone Number":     {},
		"Email":            {},
		"Amount Owed:":     {},
		"Lease ID":         {},
		"Last Edited Date": {},
		// Optional: uncomment if you want automation to manage Status too
		// "Status": {},
	}
}
