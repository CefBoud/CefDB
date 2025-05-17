package query

import (
	"fmt"
	"slices"
)

type ProjectScan struct {
	inputScan Scan
	fields    []string
}

func NewProjectScan(inputScan Scan, fields []string) *ProjectScan {
	return &ProjectScan{inputScan: inputScan, fields: fields}
}

func (ps *ProjectScan) BeforeFirst() {
	ps.inputScan.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.inputScan.Next()
}

func (ps *ProjectScan) GetInt(fldname string) (int, error) {
	ok := slices.Contains(ps.fields, fldname)
	if !ok {
		return -1, fmt.Errorf("unknown field")
	}
	return ps.inputScan.GetInt(fldname)
}

func (ps *ProjectScan) GetString(fldname string) (string, error) {
	ok := slices.Contains(ps.fields, fldname)
	if !ok {
		return "", fmt.Errorf("unknown field")
	}
	return ps.inputScan.GetString(fldname)
}

func (ps *ProjectScan) GetVal(fldname string) (any, error) {
	ok := slices.Contains(ps.fields, fldname)
	if !ok {
		return "", fmt.Errorf("unknown field")
	}
	return ps.inputScan.GetVal(fldname)
}

func (ps *ProjectScan) HasField(fldname string) bool {
	return ps.inputScan.HasField(fldname)
}

func (ps *ProjectScan) Close() {
	ps.inputScan.Close()
}
