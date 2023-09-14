package main

import (
	"encoding/binary"
	"fmt"
)

func main() {
	a := make([]byte, 5)
	c := 1000
	binary.LittleEndian.PutUint16(a, uint16(c))
	fmt.Printf("%T, %b", a[:5], a[:5])

}
