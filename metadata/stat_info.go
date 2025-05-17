package metadata

// StatInfo holds statistical information about a table:
// the number of blocks, the number of records, and an estimated
// number of distinct values for each field.
type StatInfo struct {
	NumBlocks int
	NumRecs   int
}

// NewStatInfo creates a new StatInfo object.
// The number of distinct values is not passed in — it's estimated.
func NewStatInfo(numBlocks, numRecs int) *StatInfo {
	return &StatInfo{
		NumBlocks: numBlocks,
		NumRecs:   numRecs,
	}
}

// BlocksAccessed returns the estimated number of blocks in the table.
func (s *StatInfo) BlocksAccessed() int {
	return s.NumBlocks
}

// RecordsOutput returns the estimated number of records in the table.
func (s *StatInfo) RecordsOutput() int {
	return s.NumRecs
}

// DistinctValues is a VERY rough estimate for demonstration purposes.
func (s *StatInfo) DistinctValues(fieldName string) int {
	return 1 + (s.NumRecs / 3)
}
