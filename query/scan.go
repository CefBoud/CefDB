package query

type Scan interface {
	// Position the scan before its first record.
	BeforeFirst()

	// Move the scan to the next record.
	// Returns false if there is no next record.
	Next() bool

	// Return the value of the specified integer field in the current record.
	GetInt(fldname string) (int, error)

	// Return the value of the specified string field in the current record.
	GetString(fldname string) (string, error)

	// Return the value of the specified field
	GetVal(fldname string) (any, error)

	// Return true if the scan has the specified field.
	HasField(fldname string) bool

	// Close the scan and its subscans, if any.
	Close()
}
