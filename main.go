package main

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
)

func main() {
	fmt.Println("yes")
	p := file.NewPage(4096)
	p.SetInt(0, 5)
	p.SetBytes(10, []byte{1, 2})
	p.SetString(100, "toto")

	i := p.GetInt(0)
	b := p.GetBytes(10)
	s := p.GetString(100)
	fmt.Printf("i=%v   b=%v   s=%v", i, b, s)
}
