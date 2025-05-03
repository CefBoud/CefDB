package record

// following the JDBC. Why ..
const (
	INTEGER = 4
	VARCHAR = 12
)

type FieldInfo struct {
	Type   int
	Length int
}

// Schema is contains the names, types and lengths of a table fields
type Schema struct {
	Fields map[string]FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		Fields: map[string]FieldInfo{},
	}
}

func (s *Schema) AddField(fname string, ftype int, flen int) {
	s.Fields[fname] = FieldInfo{Type: ftype, Length: flen}
}

func (s *Schema) AddIntField(fname string) {
	s.Fields[fname] = FieldInfo{Type: INTEGER}
}

func (s *Schema) AddStringField(fname string, flength int) {
	s.Fields[fname] = FieldInfo{Type: VARCHAR, Length: flength}
}

func (s *Schema) HasField(fname string) bool {
	_, ok := s.Fields[fname]
	return ok
}

func (s *Schema) FieldType(fname string) int {
	return s.Fields[fname].Type
}
func (s *Schema) FieldLength(fname string) int {
	return s.Fields[fname].Length
}
func (s *Schema) FieldSizeInBytes(fname string) int {
	if s.Fields[fname].Type == INTEGER {
		return 4
	}
	return 4 + s.Fields[fname].Length
}

func (s *Schema) Add(fname string, s2 *Schema) {
	s.AddField(fname, s2.FieldType(fname), s2.FieldLength(fname))
}

func (s *Schema) AddAll(fname string, s2 *Schema) {
	for fname, finfo := range s2.Fields {
		s.AddField(fname, finfo.Type, finfo.Length)
	}
}
