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

	page := sequentialstorage.NewPage([]byte("om"), []byte("nama shivayaa"))

	err = d.Set(page)
	if err != nil {
		panic(err)
	}

}

func testMMAP() {
	file, err := os.OpenFile("./data.txt", os.O_RDWR|os.O_CREATE, 777)
	if err != nil {
		panic(err)
	}
	fileMaxLength := 4 << (10) // 2 GB file is max size im keeping
	if err := file.Truncate(int64(fileMaxLength)); err != nil {
		fmt.Println("Error truncating file:", err)
		file.Close()
		return
	}

	fd := file.Fd()
	vmem, err := syscall.Mmap(int(fd), 0, fileMaxLength, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	c := "value"
	copy(vmem[:len(c)], []byte(c))
	if err = file.Sync(); err != nil {
		panic(err)
	}
}
