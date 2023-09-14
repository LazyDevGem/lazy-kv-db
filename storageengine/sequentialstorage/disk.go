package sequentialstorage

import (
	"fmt"
	"os"
	"syscall"
)

type disk struct {
	file *os.File //file open
	fd   int      // file descriptor
	vmem mmap     //mmap
}

type mmap struct {
	data                 [][]byte
	sizeInByteCount      int
	sizeInPages          int
	currentOffsetInBytes int
	currentOffsetInPages int
	currentIncreaseIndex int
}

func NewDisk() (*disk, error) {
	file, err := os.OpenFile("./data.txt", os.O_RDWR|os.O_CREATE, 777)
	if err != nil {
		return nil, err
	}
	fileMaxLength := 4000000 // starting sizeInByteCount - 4mb
	fd := file.Fd()
	vmem, err := syscall.Mmap(int(fd), 0, fileMaxLength, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	err = file.Truncate(int64(fileMaxLength))
	if err != nil {
		panic(err)
	}
	var chunk [][]byte
	chunk = append(chunk, vmem)
	return &disk{
		fd:   int(fd),
		file: file,
		vmem: mmap{
			sizeInByteCount:      len(vmem),
			data:                 chunk,
			currentOffsetInBytes: 0,
			currentOffsetInPages: 0,
			currentIncreaseIndex: 0,
			sizeInPages:          len(vmem) / PAGE_SIZE,
		},
	}, nil
}

func (d *disk) increaseFileSize(totalPages int) error {
	fi, err := d.file.Stat()
	if err != nil {
		return err
	}
	if int(fi.Size()/PAGE_SIZE) > totalPages {
		return nil
	}

	err = d.file.Truncate(int64(totalPages*PAGE_SIZE*1000) * 10)
	if err != nil {
		return err
	}
	return nil
}

func (d *disk) increaseMMAPSize(totalPages int) error {

	if int(d.vmem.sizeInByteCount/PAGE_SIZE) > totalPages {
		return nil
	}

	newMem, err := syscall.Mmap(int(d.fd), int64(d.vmem.sizeInByteCount), totalPages*PAGE_SIZE*1000*10, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	d.vmem.data = append(d.vmem.data, newMem)
	d.vmem.sizeInByteCount += len(newMem)
	d.vmem.sizeInPages += len(newMem)
	d.vmem.currentIncreaseIndex += 1
	return nil
}

// TODO: to handle concurrency and durability
func (d *disk) Set(p *page) error {
	if d.vmem.currentOffsetInPages+4000 > d.vmem.sizeInPages {
		fmt.Print("increasing size \n")
		err := d.increaseFileSize(d.vmem.sizeInPages + 1)
		if err != nil {
			return err
		}
		err = d.increaseMMAPSize(d.vmem.sizeInPages + 1)
		if err != nil {
			return err
		}
	}
	fmt.Print("copyign file \n")
	va := p.Serialise()
	copy(d.vmem.data[d.vmem.currentIncreaseIndex][d.vmem.currentOffsetInBytes:], va)
	fmt.Print("copyign done \n")
	d.vmem.currentOffsetInBytes += 4000
	d.vmem.currentOffsetInPages += 1
	return nil
}

func (d *disk) Get() {

}
