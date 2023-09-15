package sequentialstorage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

type disk struct {
	file        *os.File //file open
	fd          int      // file descriptor
	vmem        mmap     //mmap
	fileDetails FileDetails
}

type FileDetails struct {
	totalPages int
	totalBytes int
}
type mmap struct {
	data                 [][]byte
	freeData             []byte
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
	fileMaxLength := 16384
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
	var freeChunk []byte
	chunk = append(chunk, vmem)
	freeChunk = append(freeChunk, vmem...)
	return &disk{
		fd:   int(fd),
		file: file,
		vmem: mmap{
			sizeInByteCount:      fileMaxLength,
			data:                 chunk,
			freeData:             freeChunk,
			currentOffsetInBytes: 0,
			currentOffsetInPages: 0,
			currentIncreaseIndex: 0,
			sizeInPages:          fileMaxLength / (PAGE_SIZE),
		},
		fileDetails: FileDetails{
			totalPages: fileMaxLength / (PAGE_SIZE),
			totalBytes: fileMaxLength,
		},
	}, nil
}

func (d *disk) increaseFileSize(totalPages int) error {
	fi, err := d.file.Stat()
	if err != nil {
		return err
	}
	if int(fi.Size()/(PAGE_SIZE)) > totalPages {
		fmt.Println("not increasing", totalPages, fi.Size()/(PAGE_SIZE))
		return nil
	}

	err = d.file.Truncate(int64(totalPages*PAGE_SIZE*100) * 10)
	if err != nil {
		return err
	}
	d.fileDetails.totalBytes = totalPages * PAGE_SIZE * 10 * 10
	d.fileDetails.totalPages = totalPages * 10 * 10
	return nil
}

func (d *disk) increaseMMAPSize(totalPages int) error {

	if int(d.vmem.sizeInByteCount/(PAGE_SIZE)) > totalPages {
		return nil
	}
	//fi, _ := d.file.Stat()
	//fmt.Println("filesize", fi.Size())
	//fmt.Println("mmap next size", d.vmem.sizeInByteCount+PAGE_SIZE*1000*10)

	//check the file resizing is same as mmap resiszing

	newMem, err := syscall.Mmap(int(d.fd), int64(d.vmem.sizeInByteCount), PAGE_SIZE*10, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	d.vmem.data = append(d.vmem.data, newMem)
	d.vmem.freeData = append(d.vmem.freeData, newMem...)
	d.vmem.sizeInByteCount += len(newMem)
	d.vmem.sizeInPages += 10
	d.vmem.currentIncreaseIndex += 1
	return nil
}

func getNextOffset(val int) {
	// 16384 is the single page size

}

// TODO: to handle concurrency and durability
func (d *disk) Set(p *page) error {
	fmt.Println("vmem page offset", d.vmem.currentOffsetInPages, "vmem byte offset", d.vmem.currentOffsetInBytes, "vmem total size in pages", d.vmem.sizeInPages, "vmem total size in bytes", d.vmem.sizeInByteCount, "file total size pages", d.fileDetails.totalPages, "file total size bytes", d.fileDetails.totalBytes)
	if d.vmem.currentOffsetInPages+1 >= d.fileDetails.totalPages {
		err := d.increaseFileSize(d.vmem.sizeInPages + 1)
		if err != nil {
			return err
		}
	}

	if d.vmem.currentOffsetInPages+1 >= d.vmem.sizeInPages {
		err := d.increaseMMAPSize(d.vmem.sizeInPages + 1)
		if err != nil {
			return err
		}
	}

	va := p.Serialise()

	//copy(d.vmem.data[d.vmem.currentIncreaseIndex][d.vmem.currentOffsetInBytes:], va)
	copy(d.vmem.freeData[d.vmem.currentOffsetInBytes:], va)
	d.vmem.currentOffsetInBytes += PAGE_SIZE
	d.vmem.currentOffsetInPages += 1
	return nil
}

func (d *disk) Get(input string) (string, error) {
	for i := 0; i < d.vmem.currentOffsetInPages; i++ {
		lenKeyIdxStart := 16384*i + 100
		lenKeyIdxEnd := lenKeyIdxStart + 2
		b := make([]byte, 2)
		copy(b, d.vmem.freeData[lenKeyIdxStart:lenKeyIdxEnd])
		size := binary.LittleEndian.Uint16(b)
		dataKeyIdxStart := lenKeyIdxEnd
		dataKeyIdxEnd := lenKeyIdxEnd + int(size)
		key := d.vmem.freeData[dataKeyIdxStart:dataKeyIdxEnd]
		if string(key) == input {
			valLenIdxStart := 16384*i + 4382
			valLenIdxEnd := valLenIdxStart + 2
			c := make([]byte, 2)
			copy(c, d.vmem.freeData[valLenIdxStart:valLenIdxEnd])
			size = binary.LittleEndian.Uint16(c)
			dataValueIdxStart := valLenIdxEnd
			dataValueIdxEnd := valLenIdxEnd + int(size)

			value := d.vmem.freeData[dataValueIdxStart:dataValueIdxEnd]
			return string(value), nil
		}
	}
	return "", errors.New("no value present")
}
