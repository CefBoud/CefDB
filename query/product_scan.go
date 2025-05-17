package query

type ProductScan struct {
	leftInputScan, rightInputScan Scan
}

func NewProductScan(left Scan, right Scan) *ProductScan {
	return &ProductScan{leftInputScan: left, rightInputScan: right}
}

func (ps *ProductScan) BeforeFirst() {
	ps.leftInputScan.BeforeFirst()
	ps.leftInputScan.Next()
	ps.rightInputScan.BeforeFirst()
}

func (ps *ProductScan) Next() bool {

	if ps.rightInputScan.Next() {
		return true
	}

	ps.rightInputScan.BeforeFirst()
	return ps.rightInputScan.Next() && ps.leftInputScan.Next()

}

func (ps *ProductScan) GetInt(fldname string) (int, error) {
	if ps.leftInputScan.HasField(fldname) {
		return ps.leftInputScan.GetInt(fldname)
	}
	return ps.rightInputScan.GetInt(fldname)
}

func (ps *ProductScan) GetString(fldname string) (string, error) {
	if ps.leftInputScan.HasField(fldname) {
		return ps.leftInputScan.GetString(fldname)
	}
	return ps.rightInputScan.GetString(fldname)
}

func (ps *ProductScan) GetVal(fldname string) (any, error) {
	if ps.leftInputScan.HasField(fldname) {
		return ps.leftInputScan.GetVal(fldname)
	}
	return ps.rightInputScan.GetVal(fldname)
}

func (ps *ProductScan) HasField(fldname string) bool {
	return ps.leftInputScan.HasField(fldname) || ps.rightInputScan.HasField(fldname)
}

func (ps *ProductScan) Close() {
	ps.leftInputScan.Close()
	ps.rightInputScan.Close()
}
