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

type Cell interface {
	GetKey() uint64
	Serialize() []byte
	Size() uint32
}

type LeafCell struct {
	Key   uint64
	Value []byte
}

func NewLeafCell(key uint64, value []byte) *LeafCell {
	return &LeafCell{
		Key:   key,
		Value: value,
	}
}

func (c *LeafCell) GetKey() uint64 {
	return c.Key
}

func (c *LeafCell) Size() uint32 {
	return 8 + 4 + uint32(len(c.Value))
}

func (c *LeafCell) Serialize() []byte {
	size := c.Size()
	data := make([]byte, size)

	binary.BigEndian.PutUint64(data[0:8], c.Key)
	binary.BigEndian.PutUint32(data[8:12], uint32(len(c.Value)))
	copy(data[12:], c.Value)

	return data
}

func DeserializeLeafCell(data []byte) (*LeafCell, error) {
	if len(data) < 12 {
		return nil, errors.New("data too small for leaf cell header")
	}

	key := binary.BigEndian.Uint64(data[0:8])
	valueLen := binary.BigEndian.Uint32(data[8:12])

	if len(data) < int(12+valueLen) {
		return nil, errors.New("data too small for leaf cell value")
	}

	value := make([]byte, valueLen)
	copy(value, data[12:12+valueLen])

	return &LeafCell{
		Key:   key,
		Value: value,
	}, nil
}

type InteriorCell struct {
	Key       uint64
	ChildPage uint32
}

func NewInteriorCell(key uint64, childPage uint32) *InteriorCell {
	return &InteriorCell{
		Key:       key,
		ChildPage: childPage,
	}
}

func (c *InteriorCell) GetKey() uint64 {
	return c.Key
}

func (c *InteriorCell) Size() uint32 {
	return 12
}

func (c *InteriorCell) Serialize() []byte {
	data := make([]byte, 12)

	binary.BigEndian.PutUint64(data[0:8], c.Key)
	binary.BigEndian.PutUint32(data[8:12], c.ChildPage)

	return data
}

func DeserializeInteriorCell(data []byte) (*InteriorCell, error) {
	if len(data) < 12 {
		return nil, errors.New("data too small for interior cell")
	}

	key := binary.BigEndian.Uint64(data[0:8])
	childPage := binary.BigEndian.Uint32(data[8:12])

	return &InteriorCell{
		Key:       key,
		ChildPage: childPage,
	}, nil
}

func SerializeLeafCells(cells []*LeafCell) []byte {
	if len(cells) == 0 {
		return []byte{}
	}

	totalSize := 0
	for _, cell := range cells {
		totalSize += int(cell.Size())
	}

	data := make([]byte, 0, totalSize)
	for _, cell := range cells {
		data = append(data, cell.Serialize()...)
	}

	return data
}

func DeserializeLeafCells(data []byte) ([]*LeafCell, error) {
	cells := []*LeafCell{}
	offset := 0

	for offset < len(data) {
		if len(data)-offset < 12 {
			break
		}

		valueLen := binary.BigEndian.Uint32(data[offset+8 : offset+12])
		cellSize := 12 + int(valueLen)

		if len(data)-offset < cellSize {
			return nil, errors.New("incomplete cell data")
		}

		cell, err := DeserializeLeafCell(data[offset : offset+cellSize])
		if err != nil {
			return nil, err
		}

		cells = append(cells, cell)
		offset += cellSize
	}

	return cells, nil
}

func SerializeInteriorCells(cells []*InteriorCell) []byte {
	data := make([]byte, len(cells)*12)

	for i, cell := range cells {
		copy(data[i*12:(i+1)*12], cell.Serialize())
	}

	return data
}

func DeserializeInteriorCells(data []byte, count int) ([]*InteriorCell, error) {
	if len(data) < count*12 {
		return nil, errors.New("data too small for interior cells")
	}

	cells := make([]*InteriorCell, count)
	for i := 0; i < count; i++ {
		offset := i * 12
		cell, err := DeserializeInteriorCell(data[offset : offset+12])
		if err != nil {
			return nil, err
		}
		cells[i] = cell
	}

	return cells, nil
}
