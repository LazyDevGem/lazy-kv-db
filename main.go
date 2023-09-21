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
	for i := 0; i < 1000; i++ {
		fmt.Println("set for ", i)
		page := sequentialstorage.NewPage([]byte(fmt.Sprintf("om4-%v", i)), []byte(fmt.Sprintf("nama shivayaa - %v", i)))
		err = d.Set(page)
		if err != nil {
			panic(err)
		}
	}
	page := sequentialstorage.NewPage([]byte("hello123"), []byte("world"))
	err = d.Set(page)
	if err != nil {
		panic(err)
	}

	val, _, _, err := d.Get("hello123")
	if err != nil {
		fmt.Println("no value found")
	}
	fmt.Println("dound !!!!!!!!!!!!", val)

}

func m2ain() {
	fmt.Println(int64(syscall.Getpagesize()))
	file, err := os.OpenFile("./data.txt", os.O_RDWR|os.O_CREATE, 777)
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

//func main() {
//
//	b := []byte("asdasd")
//	var c []byte
//
//	c = append(c, b...)
//	fmt.Println(string(c[0]))
//	b[1] = '3'
//	fmt.Println(string(c[0]))
//
//}
