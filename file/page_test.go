package file

import (
	"bytes"
	"testing"
)

func TestPage(t *testing.T) {
	p := NewPage(4096)
	// Test SetInt and GetInt
	expectedInt := 15
	offsetInt := 5
	p.SetInt(offsetInt, expectedInt)
	actualInt := p.GetInt(offsetInt)
	if actualInt != expectedInt {
		t.Errorf("GetInt(%d) returned %d, expected %d", offsetInt, actualInt, expectedInt)
	}

	// Test SetBytes and GetBytes
	expectedBytes := []byte{101, 102}
	offsetBytes := 20
	p.SetBytes(offsetBytes, expectedBytes)
	actualBytes := p.GetBytes(offsetBytes)
	if !bytes.Equal(actualBytes, expectedBytes) {
		t.Errorf("GetBytes(%d) returned %v, expected %v", offsetBytes, actualBytes, expectedBytes)
	}

	// Test SetString and GetString
	expectedString := "test"
	offsetString := 100
	p.SetString(offsetString, expectedString)
	actualString := p.GetString(offsetString)
	if actualString != expectedString {
		t.Errorf("GetString(%d) returned %q, expected %q", offsetString, actualString, expectedString)
	}
}
