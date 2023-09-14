package main

import (
	"fmt"
	"os"
	"syscall"
)

func main2() {
	// Create a memory-mapped file (replace with your desired file size)
	size := int64(4096) // 4KB
	file, err := os.Create("mmap_example.txt")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	if err := file.Truncate(size); err != nil {
		fmt.Println("Error truncating file:", err)
		file.Close()
		return
	}

	// Memory-mapping the file
	mmapData, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		fmt.Println("Error mapping file to memory:", err)
		file.Close()
		return
	}
	defer syscall.Munmap(mmapData)

	// Copy a value (e.g., string) to the memory-mapped file
	valueToCopy := []byte("Hello, Memory-Mapped File!")
	copy(mmapData[:len(valueToCopy)], valueToCopy)

	// Ensure changes are flushed to disk
	err = file.Sync()
	if err != nil {
		panic(err)
	}

	fmt.Println("Data written to memory-mapped file:", string(mmapData[:len(valueToCopy)]))
}
