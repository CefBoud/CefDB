package record

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/tx"
)

const (
	EMPTY = 0
	USED  = 1
)

type RecordPage struct {
	Tx     *tx.Transaction
	Blk    *file.BlockId
	Layout *Layout
}

func NewRecordPage(tx *tx.Transaction, blk *file.BlockId, layout *Layout) (*RecordPage, error) {
	err := tx.Pin(blk)
	if err != nil {
		return nil, err
	}
	return &RecordPage{
		Tx:     tx,
		Blk:    blk,
		Layout: layout,
	}, nil
}

func (rp *RecordPage) GetInt(slot int, fname string) (int, error) {
	offset := rp.Offset(slot) + rp.Layout.Offset(fname)
	v, err := rp.Tx.GetInt(rp.Blk, offset)
	if err != nil {
		return 0, fmt.Errorf("recordPage GetInt error: %v", err)
	}
	return v, nil
}
func (rp *RecordPage) GetString(slot int, fname string) (string, error) {
	offset := rp.Offset(slot) + rp.Layout.Offset(fname)
	v, err := rp.Tx.GetString(rp.Blk, offset)
	if err != nil {
		return "", fmt.Errorf("recordPage GetString error: %v", err)
	}
	return v, nil
}

func (rp *RecordPage) SetInt(slot int, fname string, val int) error {
	offset := rp.Offset(slot) + rp.Layout.Offset(fname)
	err := rp.Tx.SetInt(rp.Blk, offset, val, true)
	if err != nil {
		return fmt.Errorf("recordPage SetInt error: %v", err)
	}
	return nil
}

func (rp *RecordPage) SetString(slot int, fname string, val string) error {
	offset := rp.Offset(slot) + rp.Layout.Offset(fname)
	err := rp.Tx.SetString(rp.Blk, offset, val, true)
	if err != nil {
		return fmt.Errorf("recordPage SetString error: %v", err)
	}
	return nil
}

func (rp *RecordPage) Delete(slot int) error {
	return rp.Tx.SetInt(rp.Blk, rp.Offset(slot), EMPTY, true)
}

func (rp *RecordPage) IsValidSlot(slot int) bool {
	// can the next slot start within the current block?
	return rp.Offset(slot+1) < rp.Tx.BlockSize()
}

// Offset returns the given slot's offset within the page
func (rp *RecordPage) Offset(slot int) int {
	return rp.Layout.SlotSize * slot
}

// SearchAfter returns the next slot that comes after `slot` whose flag is `flag`
func (rp *RecordPage) SearchAfter(slot, flag int) int {
	slot++
	for rp.IsValidSlot(slot) {
		f, err := rp.Tx.GetInt(rp.Blk, rp.Offset(slot))
		if err != nil {
			return -1
		} else if f == flag {
			return slot
		}
		slot++
	}
	return -1
}

func (rp *RecordPage) Format() error {
	slot := 0
	var err error
	for rp.IsValidSlot(slot) {
		err = rp.Tx.SetInt(rp.Blk, rp.Offset(slot), EMPTY, false)
		if err != nil {
			return fmt.Errorf("recordPage Format error: %v", err)
		}
		slot++
	}
	return nil
}

func (rp *RecordPage) NextAfter(slot int) int {
	return rp.SearchAfter(slot, USED)
}

func (rp *RecordPage) InsertAfter(slot int) (int, error) {
	s := rp.SearchAfter(slot, EMPTY)
	if s > -1 {
		err := rp.Tx.SetInt(rp.Blk, rp.Offset(s), USED, true)
		if err != nil {
			return -1, fmt.Errorf("recordPage InsertAfter error: %v", err)
		}
	}
	return s, nil
}
