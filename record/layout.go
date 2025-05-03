package record

// Layout is the struct of a record. It determines its slotsize and the offset of each field
type Layout struct {
	Schema   *Schema
	Offsets  map[string]int
	SlotSize int
}

func NewLayout(s *Schema) *Layout {
	pos := 4 // 4 bytes for inuse flag
	offset := make(map[string]int)
	for fname := range s.Fields {
		offset[fname] = pos
		pos += s.FieldSizeInBytes(fname)
	}

	return &Layout{
		Schema:   s,
		Offsets:  offset,
		SlotSize: pos,
	}
}

func (l *Layout) Offset(fname string) int {
	return l.Offsets[fname]
}
