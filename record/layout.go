package record

import "sort"

// Layout is the struct of a record. It determines its slotsize and the offset of each field
type Layout struct {
	Schema   *Schema
	Offsets  map[string]int
	SlotSize int
}

func NewLayout(s *Schema) *Layout {
	pos := 4 // 4 bytes for inuse flag
	offset := make(map[string]int)

	// we sort the strings alphabetically
	// ideally, other considerations such as memory alignment
	var orderedFields []string
	for f := range s.Fields {
		orderedFields = append(orderedFields, f)
	}
	sort.Strings(orderedFields)
	for _, fname := range orderedFields {
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
