package main

import (
	"fmt"
	"lazy-kv-db/storageengine/sequentialstorage"
	"os"
	"syscall"
)

func main() {
	d, err := sequentialstorage.NewDisk()
	if err != nil {
		panic(err)
	}

	for i := 0; i < 2; i++ {
		page := sequentialstorage.NewPage([]byte(fmt.Sprintf("om-%v", i)), []byte(fmt.Sprintf("nama shivayaa - %v", i)))
		err = d.Set(page)
		if err != nil {
			panic(err)
		}
	}

	val, err := d.Get("om-0")
	if err != nil {
		fmt.Println("no value found")
	}
	fmt.Print(val)
}

func testMMAP() {
	file, err := os.Open("./data2.txt")
	if err != nil {
		panic(err)
	}
	fileMaxLength := 8000 // 2 GB file is max size im keeping
	if err := file.Truncate(int64(fileMaxLength)); err != nil {
		fmt.Println("Error truncating file:", err)
		file.Close()
		return
	}
	file.Sync()
	vmem, err := syscall.Mmap(int(file.Fd()), 1, 8001, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	c := "value"
	copy(vmem[:len(c)], []byte(c))
	if err = file.Sync(); err != nil {
		panic(err)
	}
}
