package sequentialstorage

import (
	"os"
	"syscall"
)

type disk struct {
	file *os.File //file open
	fd   int      // file descriptor
	vmem mmap     //mmap
}

type mmap struct {
	data          [][]byte
	size          int
	currentOffset int
}

func NewDisk() (*disk, error) {
	file, err := os.Open("./data")
	if err != nil {
		return nil, err
	}
	fileMaxLength := 4 << (10 * 2) // starting size - 4mb
	fd := file.Fd()
	vmem, err := syscall.Mmap(int(fd), 0, fileMaxLength, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	var chunk [][]byte
	chunk = append(chunk, vmem)
	return &disk{
		fd: int(fd),
		vmem: mmap{
			size:          len(vmem),
			data:          chunk,
			currentOffset: 0,
		},
	}, nil
}

func (d *disk) increaseMMAPSize(totalPages int) error {
	fi, err := d.file.Stat()
	if err != nil {
		return err
	}
	if int(fi.Size()/PAGE_SIZE) > totalPages {
		return nil
	}

	err = d.file.Truncate(int64(totalPages * PAGE_SIZE * 1000))
	if err != nil {
		return err
	}

	newMem, err := syscall.Mmap(int(d.fd), 0, totalPages*PAGE_SIZE*1000, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	d.vmem.data = append(d.vmem.data, newMem)
	d.vmem.size += len(newMem)
	return nil
}

func (d *disk) Set(p *page) {

}

func Get() {

}
