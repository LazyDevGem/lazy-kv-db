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
	SizeInByteCount      int `json:"size_in_byte_count"`
	CurrentIncreaseIndex int `json:"current_increase_index"`
	CurrentIndexOffset   int `json:"current_index_offset"`
}

type disk struct {
	file        *os.File //file open
	metaFile    *os.File
	fd          int  // file descriptor
	vmem        mmap //mmap
	fileDetails FileDetails
	tombstoning bool
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
	currentIndexOffset   int
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

		fileMaxLength := PAGE_SIZE
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
			fd:          int(fd),
			tombstoning: true,
			file:        file,
			metaFile:    metafile,
			vmem: mmap{
				sizeInByteCount:      fileMaxLength,
				data:                 chunk,
				freeData:             freeChunk,
				currentOffsetInBytes: 0,
				currentOffsetInPages: 0,
				currentIncreaseIndex: 0,
				currentIndexOffset:   0,
				sizeInPages:          fileMaxLength / (PAGE_SIZE),
			},
			fileDetails: FileDetails{
				totalPages: fileMaxLength / (PAGE_SIZE),
				totalBytes: fileMaxLength,
			},
		}, nil
	} else {
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
		return &disk{
			fd:          int(fd),
			file:        file,
			tombstoning: true,
			metaFile:    metadataFile,
			vmem: mmap{
				sizeInByteCount:      metadata.SizeInByteCount,
				data:                 chunk,
				freeData:             freeChunk,
				currentOffsetInBytes: metadata.CurrentOffsetInBytes,
				currentOffsetInPages: metadata.CurrentOffsetInBytes / PAGE_SIZE,
				currentIncreaseIndex: 0,
				currentIndexOffset:   0,
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

	err = d.file.Truncate(int64(totalPages * PAGE_SIZE * 20))
	if err != nil {
		return err
	}
	d.fileDetails.totalBytes = totalPages * PAGE_SIZE * 20
	d.fileDetails.totalPages = totalPages * 20
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

	d.vmem.sizeInByteCount += PAGE_SIZE * 10
	d.vmem.sizeInPages += 10
	d.vmem.currentIncreaseIndex += 1
	d.vmem.currentIndexOffset = 0
	return nil
}

func getNextOffset(val int) {
	// 16384 is the single page size

}

// TODO: to handle concurrency and durability
func (d *disk) Set(p *page) error {
	fmt.Println("vmem page offset", d.vmem.currentOffsetInPages, "vmem byte offset", d.vmem.currentOffsetInBytes, "vmem total size in pages", d.vmem.sizeInPages, "vmem total size in bytes", d.vmem.sizeInByteCount, "file total size pages", d.fileDetails.totalPages, "file total size bytes", d.fileDetails.totalBytes, "currentoffset index", d.vmem.currentIndexOffset, d.vmem.currentIncreaseIndex)
	_, _, _, err := d.Get(string(p.key))
	if err != nil {
		if err.Error() != "no value present" {
			return errors.New("key already exists")
		}
	}

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

	copy(d.vmem.data[d.vmem.currentIncreaseIndex][d.vmem.currentIndexOffset:], va)
	//copy(d.vmem.freeData[d.vmem.currentOffsetInBytes:], va)
	d.vmem.currentOffsetInBytes += PAGE_SIZE
	d.vmem.currentIndexOffset += PAGE_SIZE
	d.vmem.currentOffsetInPages += 1

	meta := Metadata{
		CurrentOffsetInBytes: d.vmem.currentOffsetInBytes,
		CurrentIncreaseIndex: d.vmem.currentIncreaseIndex,
		SizeInByteCount:      d.vmem.sizeInByteCount,
		CurrentIndexOffset:   d.vmem.currentIndexOffset,
	}

	err = d.file.Sync()
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

// TODO: to handle concurrency and durability if tombstoning, resizing disks have to take palce
func (d *disk) Del(input string) error {
	_, idx, startIndex, err := d.Get(input)
	if err != nil {
		return err
	}
	if !d.tombstoning {
		fmt.Println("deletion index", idx, startIndex)
		values := d.vmem.data[idx][startIndex : startIndex+PAGE_SIZE]
		for i, _ := range values {
			values[i] = '0'
		}

		err = d.file.Sync()
		if err != nil {
			return err
		}

		return nil
	}

	d.vmem.data[idx][startIndex] = TOMBSTONE
	err = d.file.Sync()
	if err != nil {
		return err
	}

	return nil

}

// TODO: to handle isolation levels
func (d *disk) Get(input string) (string, int, int, error) {
	for idx, page := range d.vmem.data {
		maxSizeOfIndexMap := len(page)
		for i := 0; i < maxSizeOfIndexMap; i = i + PAGE_SIZE {
			lenKeyIdxStart := i + 100
			lenKeyIdxEnd := lenKeyIdxStart + 2
			b := make([]byte, 2)
			copy(b, d.vmem.data[idx][lenKeyIdxStart:lenKeyIdxEnd])
			size := binary.LittleEndian.Uint16(b)
			dataKeyIdxStart := lenKeyIdxEnd
			dataKeyIdxEnd := lenKeyIdxEnd + int(size)
			key := d.vmem.data[idx][dataKeyIdxStart:dataKeyIdxEnd]

			if string(key) == input {
				valLenIdxStart := i + 4382
				valLenIdxEnd := valLenIdxStart + 2
				c := make([]byte, 2)
				copy(c, d.vmem.data[idx][valLenIdxStart:valLenIdxEnd])
				size = binary.LittleEndian.Uint16(c)
				dataValueIdxStart := valLenIdxEnd
				dataValueIdxEnd := valLenIdxEnd + int(size)

				value := d.vmem.data[idx][dataValueIdxStart:dataValueIdxEnd]
				if d.vmem.data[idx][i] == TOMBSTONE {
					continue
				}
				return string(value), idx, i, nil
			}
		}
	}
	return "", 0, 0, errors.New("no value present")
}
