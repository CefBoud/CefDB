package file

import (
	"encoding/binary"
)

var Encoding = binary.BigEndian

// Page represents a fixed-size block of data.
type Page struct {
	bb []byte
}

// NewPage creates a new Page with a direct byte buffer of the specified size.
func NewPage(blockSize int) *Page {
	return &Page{
		bb: make([]byte, blockSize),
	}
}

// NewPageFromBytes creates a new Page wrapping the given byte slice.
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		bb: b,
	}
}

// GetInt reads an integer from the specified offset.
func (p *Page) GetInt(offset int) int {
	return int(Encoding.Uint32(p.bb[offset:]))
}

// SetInt writes an integer to the specified offset.
func (p *Page) SetInt(offset int, n int) {
	Encoding.PutUint32(p.bb[offset:], uint32(n))
}

// GetBytes reads a byte slice from the specified offset.
func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.bb[offset+4 : offset+4+length]
}

// SetBytes writes a byte slice to the specified offset.
func (p *Page) SetBytes(offset int, b []byte) {
	p.SetInt(offset, len(b))
	copy(p.bb[offset+4:], b)
}

// GetString reads a string from the specified offset.
func (p *Page) GetString(offset int) string {
	length := p.GetInt(offset)
	return string(p.bb[offset+4 : offset+4+length])
}

// SetString writes a string to the specified offset.
func (p *Page) SetString(offset int, s string) {
	p.SetInt(offset, len(s))
	copy(p.bb[offset+4:], s)
}

// MaxLength calculates the maximum number of bytes required to store a string of the given length.
func MaxLength(strlen int) int {
	return 4 + strlen
}

// Contents returns the underlying bytes
func (p *Page) Contents() []byte {
	return p.bb
}
