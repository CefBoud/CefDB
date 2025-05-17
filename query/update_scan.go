package query

import "github.com/CefBoud/CefDB/record"

// UpdateScan extends Scan and includes methods for modifying records.
type UpdateScan interface {
	Scan

	// Modify the field value of the current record.
	SetVal(fldname string, val any) error

	// Modify the field value of the current record with an int.
	SetInt(fldname string, val int) error

	// Modify the field value of the current record with a string.
	SetString(fldname string, val string) error

	// Insert a new record somewhere in the scan.
	Insert() error

	// Delete the current record from the scan.
	Delete() error

	// Return the id of the current record.
	GetRid() record.RID

	// Position the scan at the specified record id.
	MoveToRID(rid record.RID) error
}
