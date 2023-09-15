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

	for i := 0; i < 500; i++ {
		fmt.Println("set for ", i)
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
	fmt.Println(val)

	val, err = d.Get("om-444")
	if err != nil {
		fmt.Println("no value found")
	}

	fmt.Println(val)
}

func m2ain() {
	fmt.Println(int64(syscall.Getpagesize()))
	file, err := os.OpenFile("./data2.txt", os.O_RDWR|os.O_CREATE, 777)
	a := make([]byte, 20)
	if err != nil {
		panic(err)
	}
	fileMaxLength := 10
	if err := file.Truncate(int64(fileMaxLength)); err != nil {
		fmt.Println("Error truncating file:", err)
		file.Close()
		return
	}

	vmem, err := syscall.Mmap(int(file.Fd()), 0, 10, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	a = vmem
	v := "1234567891"
	fmt.Println(a)

	copy(a[:], []byte(v))
	fmt.Println(a)
	fmt.Println(vmem)
	fmt.Println(len(a))
	fmt.Println(len(vmem))
	if err = file.Sync(); err != nil {
		panic(err)
	}
	if err := file.Truncate(int64(20000)); err != nil {
		fmt.Println("Error truncating file:", err)
		file.Close()
		return
	}

	vmem, err = syscall.Mmap(int(file.Fd()), 16384, 406, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	a = append(a, vmem...)
	c := "value"
	copy(vmem[:len(c)], []byte(c))
	if err = file.Sync(); err != nil {
		panic(err)
	}
}

func mai2n() {
	a := make([]byte, 2)
	b := []byte("asdasd")
	copy(a, b)

	fmt.Println(a)
	fmt.Println(b)
	var c byte
	c = 'D'
	a[1] = c

	fmt.Println(a)
	fmt.Println(b)
}
