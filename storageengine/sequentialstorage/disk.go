package sequentialstorage

import (
	"encoding/binary"
	"errors"
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
	fileMaxLength := 4000
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
			sizeInByteCount:      len(vmem),
			data:                 chunk,
			freeData:             freeChunk,
			currentOffsetInBytes: 0,
			currentOffsetInPages: 0,
			currentIncreaseIndex: 0,
			sizeInPages:          len(vmem) / (PAGE_SIZE * 1000),
		},
	}, nil
}

func (d *disk) increaseFileSize(totalPages int) error {
	fi, err := d.file.Stat()
	if err != nil {
		return err
	}
	if int(fi.Size()/(PAGE_SIZE*1000)) > totalPages {
		return nil
	}

	err = d.file.Truncate(int64(totalPages*PAGE_SIZE*1000*10) * 10)
	if err != nil {
		return err
	}
	return nil
}

func (d *disk) increaseMMAPSize(totalPages int) error {

	if int(d.vmem.sizeInByteCount/(PAGE_SIZE*1000)) > totalPages {
		return nil
	}

	//check the file resizing is same as mmap resiszing
	newMem, err := syscall.Mmap(int(d.fd), 0, d.vmem.sizeInByteCount+PAGE_SIZE*1000*10, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
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

// TODO: to handle concurrency and durability
func (d *disk) Set(p *page) error {
	if d.vmem.currentOffsetInPages+1 >= d.vmem.sizeInPages {
		err := d.increaseFileSize(d.vmem.sizeInPages + 1)
		if err != nil {
			return err
		}
		err = d.increaseMMAPSize(d.vmem.sizeInPages + 1)
		if err != nil {
			return err
		}
	}

	va := p.Serialise()

	//copy(d.vmem.data[d.vmem.currentIncreaseIndex][d.vmem.currentOffsetInBytes:], va)
	copy(d.vmem.freeData[d.vmem.currentOffsetInBytes:], va)
	d.vmem.currentOffsetInBytes += 4000
	d.vmem.currentOffsetInPages += 1
	return nil
}

func (d *disk) Get(input string) (string, error) {
	for i := 0; i < d.vmem.currentOffsetInPages; i++ {
		lenKeyIdxStart := 4000*i + 100
		lenKeyIdxEnd := lenKeyIdxStart + 2
		b := make([]byte, 2)
		copy(b, d.vmem.freeData[lenKeyIdxStart:lenKeyIdxEnd])
		size := binary.LittleEndian.Uint16(b)
		dataKeyIdxStart := lenKeyIdxEnd
		dataKeyIdxEnd := lenKeyIdxEnd + int(size)
		key := d.vmem.freeData[dataKeyIdxStart:dataKeyIdxEnd]

		if string(key) == input {
			valLenIdxStart := 4000*i + 602
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
