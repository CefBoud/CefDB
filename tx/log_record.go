package tx

import "github.com/CefBoud/CefDB/file"

// LogRecord defines the interface for different types of log records.
type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Transaction)
	String() string
}

// Constants for log record types.
const (
	CHECKPOINT = 0
	START      = 1
	COMMIT     = 2
	ROLLBACK   = 3
	SETINT     = 4
	SETSTRING  = 5
)

// // logRecordFactories maps log record types to their creation functions.
// var logRecordFactories = map[int]LogRecordCreator{
// 	CHECKPOINT: func(p *Page) LogRecord { return new(CheckpointRecord) },
// 	START:      func(p *Page) LogRecord { return NewStartRecord(p) },
// 	COMMIT:     func(p *Page) LogRecord { return NewCommitRecord(p) },
// 	ROLLBACK:   func(p *Page) LogRecord { return NewRollbackRecord(p) },
// 	SETINT:     func(p *Page) LogRecord { return NewSetIntRecord(p) },
// 	SETSTRING:  func(p *Page) LogRecord { return NewSetStringRecord(p) },
// }

// CreateLogRecord interprets the bytes returned by the log iterator and
// creates the corresponding LogRecord.
func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageFromBytes(bytes)
	switch p.GetInt(0) {
	case CHECKPOINT:
		return NewCheckpointRecord(bytes)
	case START:
		return NewStartRecord(bytes)
	case COMMIT:
		return NewCommitRecord(bytes)
	case ROLLBACK:
		return NewRollbackRecord(bytes)
	case SETINT:
		return NewSetIntRecord(bytes)
	case SETSTRING:
		return NewSetStringRecord(bytes)
	default:
		return nil
	}
}
