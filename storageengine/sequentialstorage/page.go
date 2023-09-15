package sequentialstorage

import (
	"encoding/binary"
	"errors"
)

// assuming sizeInByteCount is 4KB
type page struct {
	padding []byte //100 byte
	lenKey  []byte // 2byte
	key     []byte // depends
	lenVal  []byte // 2byte
	value   []byte // depends
}

func NewPage(key []byte, value []byte) *page {
	paddingData := make([]byte, 100)
	binary.LittleEndian.PutUint16(paddingData, 0)

	lenKey := make([]byte, 2)
	binary.LittleEndian.PutUint16(lenKey, uint16(len(key)))

	lenValue := make([]byte, 2)
	binary.LittleEndian.PutUint16(lenValue, uint16(len(value)))

	paddedKey := make([]byte, 4280) // they sum upto 16384 which is byte size is mac
	copy(paddedKey[:len(key)], key)

	paddedVal := make([]byte, 12000)
	copy(paddedVal[:len(value)], value)

	return &page{
		padding: paddingData,
		lenKey:  lenKey,
		key:     paddedKey,
		lenVal:  lenValue,
		value:   paddedVal,
	}
}

func (p *page) Valid() error {
	if len(p.padding) != 100 {
		return errors.New("wrong padding length")
	}
	if len(p.lenVal) != 2 {
		return errors.New("wrong key length")
	}
	if len(p.lenKey) != 2 {
		return errors.New("wrong value length")
	}
	return nil
}

func (p *page) getPadding() []byte {
	return p.padding
}

func (p *page) GetLenKey() ([]byte, uint16) {
	return p.lenKey, binary.LittleEndian.Uint16(p.lenKey)
}

func (p *page) GetLenVal() ([]byte, uint16) {
	return p.lenVal, binary.LittleEndian.Uint16(p.lenVal)
}

func (p *page) Key() ([]byte, string) {
	return p.key, string(p.key)
}

func (p *page) Value() ([]byte, string) {
	return p.value, string(p.value)
}

func (p *page) Serialise() []byte {
	a := append(append(append(append(p.padding, p.lenKey...), p.key...), p.lenVal...), p.value...)
	return a
}
