package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayout(t *testing.T) {
	s := NewSchema()
	s.AddIntField("myInt")
	s.AddStringField("myString", 10)
	s.AddStringField("myString2", 30)
	l := NewLayout(s)

	assert.Equal(t, 4, l.Offset("myInt"))
	assert.Equal(t, 8, l.Offset("myString"))
	assert.Equal(t, 22, l.Offset("myString2"))
	assert.Equal(t, 56, l.SlotSize)
}
