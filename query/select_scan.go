package query

import "github.com/CefBoud/CefDB/record"

type SelectScan struct {
	inputScan UpdateScan
	predicate *Predicate
}

func NewSelectScan(inputScan UpdateScan, predicate *Predicate) *SelectScan {
	return &SelectScan{inputScan: inputScan, predicate: predicate}
}

func (ps *SelectScan) BeforeFirst() {
	ps.inputScan.BeforeFirst()
}

func (ps *SelectScan) Next() bool {
	for ps.inputScan.Next() {
		ok, _ := ps.predicate.IsSatisfied(ps.inputScan)
		if ok {
			return true
		}
	}
	return false
}

func (ps *SelectScan) GetInt(fldname string) (int, error) {
	return ps.inputScan.GetInt(fldname)
}

func (ps *SelectScan) GetString(fldname string) (string, error) {
	return ps.inputScan.GetString(fldname)
}

func (ps *SelectScan) GetVal(fldname string) (any, error) {
	return ps.inputScan.GetVal(fldname)
}

func (ps *SelectScan) HasField(fldname string) bool {
	return ps.inputScan.HasField(fldname)
}

func (ps *SelectScan) Close() {
	ps.inputScan.Close()
}

func (ps *SelectScan) SetVal(fldname string, val any) error {
	return ps.inputScan.SetVal(fldname, val)
}
func (ps *SelectScan) SetInt(fldname string, val int) error {
	return ps.inputScan.SetInt(fldname, val)
}
func (ps *SelectScan) SetString(fldname string, val string) error {
	return ps.inputScan.SetString(fldname, val)
}

func (ps *SelectScan) Insert() error {
	return ps.inputScan.Insert()
}
func (ps *SelectScan) Delete() error {
	return ps.inputScan.Delete()
}
func (ps *SelectScan) GetRid() record.RID {
	return ps.inputScan.GetRid()
}
func (ps *SelectScan) MoveToRID(rid record.RID) error {
	return ps.inputScan.MoveToRID(rid)
}
