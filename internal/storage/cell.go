package storage

/*
** Cells are the individual records stored within pages, and come in two varieties: leaf cells
** (which contain actual data) and interior cells (which contain routing info).
**
** CELL TYPES
** ----------
**
** LEAF CELL - Stores actual key-value data
**   Format: [8 bytes key][4 bytes value length][N bytes value]
**
**   Structure:
**     0-7:   uint64 key (the record's unique identifier)
**     8-11:  uint32 value length (size of the value in bytes)
**     12+:   []byte value (the actual data)
**
**   Example: A user record might have:
**     - Key: 12345 (user ID)
**     - Value: {"name":"Alice","email":"alice@example.com"}
**
**   Leaf cells are stored in leaf pages, which form the bottom level of the
**   B-tree and contain all the actual database records.
**
** INTERIOR CELL - Stores navigation information
**   Format: [8 bytes key][4 bytes child page pointer]
**
**   Structure:
**     0-7:   uint64 key (minimum key in the child page)
**     8-11:  uint32 child page number (pointer to child page)
**
**   Example: An interior cell with key=100 and childPage=5 means:
**     "All records with keys >= 100 can be found by following page 5"
**
**   Interior cells are stored in interior pages, which form the upper levels
**   of the B-tree and provide routing to efficiently locate records.
**
** B-TREE NAVIGATION
** -----------------
** When searching for a key:
** 1. Start at root (interior page)
** 2. Binary search the cells to find which child pointer to follow
** 3. Navigate down through interior pages
** 4. Reach a leaf page and find the actual data
**
** Example tree structure:
**                     [Interior Page - Root]
**                     Cell: key=50, page=2
**                     Cell: key=100, page=3
**                    /                      \
**         [Leaf Page 2]              [Leaf Page 3]
**         keys: 10,20,30,40          keys: 50,60,70,80
**
** SERIALIZATION
** -------------
** Both cell types implement the Cell interface with methods:
** - GetKey() - Returns the cell's key
** - Size() - Returns the serialized size in bytes
** - Serialize() - Converts cell to byte array for storage
**
** All multi-byte integers use Big Endian encoding for cross-platform
** compatibility and to maintain sorted byte order.
**
** BATCH OPERATIONS
** ----------------
** Helper functions are provided for bulk serialization:
** - SerializeLeafCells - Concatenates multiple leaf cells
** - DeserializeLeafCells - Extracts multiple leaf cells from byte stream
** - SerializeInteriorCells - Concatenates multiple interior cells
** - DeserializeInteriorCells - Extracts fixed count of interior cells
**
** These are useful for page splits, merges, and bulk operations.
 */

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

// Serialize converts the leaf cell to bytes
// Format: [key_length: 4 bytes][key_data: variable][value_length: 4 bytes][value_data: variable]
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

// Serialize converts the interior cell to bytes
// Format: [child_page: 4 bytes][key_length: 4 bytes][key_data: variable]
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
