package sequentialstorage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
)

type Metadata struct {
	CurrentOffsetInBytes int `json:"current_offset_in_bytes"`
	CurrentIncreaseIndex int `json:"current_increase_index"`
	SizeInByteCount      int `json:"size_in_byte_count"`
}

type disk struct {
	file        *os.File //file open
	metaFile    *os.File
	fd          int  // file descriptor
	vmem        mmap //mmap
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
	_, error := os.Stat("./data.txt")
	if os.IsNotExist(error) {
		file, err := os.OpenFile("./data.txt", os.O_RDWR|os.O_CREATE, 777)
		if err != nil {
			return nil, err
		}

		metafile, err := os.OpenFile("./metadata.json", os.O_RDWR|os.O_CREATE, 777)
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
		freeChunk := vmem
		chunk = append(chunk, vmem)
		return &disk{
			fd:       int(fd),
			file:     file,
			metaFile: metafile,
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
	} else {
		fmt.Println("hi")
		file, err := os.OpenFile("./data.txt", os.O_RDWR|os.O_CREATE, 777)
		if err != nil {
			return nil, err
		}
		metadataFile, err := os.OpenFile("./metadata.json", os.O_RDWR|os.O_CREATE, 777)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}

		fileByt, err := ioutil.ReadAll(metadataFile)
		if err != nil {
			return nil, err
		}
		var metadata Metadata
		err = json.Unmarshal(fileByt, &metadata)
		if err != nil {
			return nil, err
		}

		fd := file.Fd()

		vmem, err := syscall.Mmap(int(fd), 0, metadata.SizeInByteCount, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}

		var chunk [][]byte
		freeChunk := vmem
		chunk = append(chunk, vmem)
		fmt.Println("size of freechunk", len(freeChunk), "size of metadataa", metadata.SizeInByteCount)
		return &disk{
			fd:       int(fd),
			file:     file,
			metaFile: metadataFile,
			vmem: mmap{
				sizeInByteCount:      metadata.SizeInByteCount,
				data:                 chunk,
				freeData:             freeChunk,
				currentOffsetInBytes: metadata.CurrentOffsetInBytes,
				currentOffsetInPages: metadata.CurrentOffsetInBytes / PAGE_SIZE,
				currentIncreaseIndex: metadata.CurrentIncreaseIndex,
				sizeInPages:          metadata.SizeInByteCount / (PAGE_SIZE),
			},
			fileDetails: FileDetails{
				totalPages: int(stat.Size() / (PAGE_SIZE)),
				totalBytes: int(stat.Size()),
			},
		}, nil
	}
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
	newMem, err := syscall.Mmap(int(d.fd), int64(d.vmem.sizeInByteCount), PAGE_SIZE*10, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	d.vmem.data = append(d.vmem.data, newMem)
	
	d.vmem.freeData = append(d.vmem.freeData, newMem...)
	d.vmem.sizeInByteCount += PAGE_SIZE * 10
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

	meta := Metadata{
		CurrentOffsetInBytes: d.vmem.currentOffsetInBytes,
		CurrentIncreaseIndex: d.vmem.currentIncreaseIndex,
		SizeInByteCount:      d.vmem.sizeInByteCount,
	}

	err := d.file.Sync()
	if err != nil {
		return err
	}

	val, _ := json.Marshal(meta)
	_, err = d.metaFile.WriteAt(val, 0)
	if err != nil {
		return err
	}

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
		fmt.Println("key", string(key), dataKeyIdxStart, lenKeyIdxStart, lenKeyIdxEnd)
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
