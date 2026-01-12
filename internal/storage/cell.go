package storage

import (
	"encoding/binary"
	"errors"
)

type LeafCell struct {
	Key   Key
	Value []byte
}

func NewLeafCell(key Key, value []byte) *LeafCell {
	return &LeafCell{
		Key:   key,
		Value: value,
	}
}

func (c *LeafCell) Serialize() []byte {
	keyData := c.Key.Encode()
	keyLen := len(keyData)
	valueLen := len(c.Value)

	totalSize := 4 + keyLen + 4 + valueLen
	buf := make([]byte, totalSize)

	binary.BigEndian.PutUint32(buf[0:4], uint32(keyLen))

	copy(buf[4:4+keyLen], keyData)

	binary.BigEndian.PutUint32(buf[4+keyLen:8+keyLen], uint32(valueLen))

	copy(buf[8+keyLen:], c.Value)

	return buf
}

func DeserializeLeafCell(data []byte) (*LeafCell, error) {
	if len(data) < 8 {
		return nil, errors.New("leaf cell data too short")
	}

	keyLen := binary.BigEndian.Uint32(data[0:4])
	if len(data) < int(4+keyLen+4) {
		return nil, errors.New("invalid leaf cell data")
	}

	keyData := data[4 : 4+keyLen]
	key, err := DecodeKey(keyData)
	if err != nil {
		return nil, err
	}

	valueLen := binary.BigEndian.Uint32(data[4+keyLen : 8+keyLen])
	if len(data) < int(8+keyLen+valueLen) {
		return nil, errors.New("value data truncated")
	}

	value := make([]byte, valueLen)
	copy(value, data[8+keyLen:8+keyLen+valueLen])

	return &LeafCell{
		Key:   key,
		Value: value,
	}, nil
}

func (c *LeafCell) Size() uint32 {
	keyData := c.Key.Encode()
	return uint32(4 + len(keyData) + 4 + len(c.Value))
}

type InteriorCell struct {
	Key       Key
	ChildPage uint32
}

func NewInteriorCell(key Key, childPage uint32) *InteriorCell {
	return &InteriorCell{
		Key:       key,
		ChildPage: childPage,
	}
}

func (c *InteriorCell) Serialize() []byte {
	keyData := c.Key.Encode()
	keyLen := len(keyData)

	totalSize := 4 + 4 + keyLen
	buf := make([]byte, totalSize)

	binary.BigEndian.PutUint32(buf[0:4], c.ChildPage)

	binary.BigEndian.PutUint32(buf[4:8], uint32(keyLen))

	copy(buf[8:], keyData)

	return buf
}

func DeserializeInteriorCell(data []byte) (*InteriorCell, error) {
	if len(data) < 8 {
		return nil, errors.New("interior cell data too short")
	}

	childPage := binary.BigEndian.Uint32(data[0:4])

	keyLen := binary.BigEndian.Uint32(data[4:8])
	if len(data) < int(8+keyLen) {
		return nil, errors.New("invalid interior cell data")
	}

	keyData := data[8 : 8+keyLen]
	key, err := DecodeKey(keyData)
	if err != nil {
		return nil, err
	}

	return &InteriorCell{
		Key:       key,
		ChildPage: childPage,
	}, nil
}

func (c *InteriorCell) Size() uint32 {
	keyData := c.Key.Encode()
	return uint32(4 + 4 + len(keyData))
}
