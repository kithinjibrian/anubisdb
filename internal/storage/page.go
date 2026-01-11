package storage

/*
** Pages are the fundamental unit of storage, containing either leaf or interior nodes of the B-tree.
**
** PAGE STRUCTURE
** --------------
** Each page follows this layout:
**
**       |----------------|
**       | page header    |   8 bytes for leaves, 12 bytes for interior nodes
**       |----------------|
**       | cell pointer   |   |  2 bytes per cell. Sorted order.
**       | array          |   |  Grows downward
**       |                |   v
**       |----------------|
**       | unallocated    |
**       | space          |
**       |----------------|   ^  Grows upwards
**       | cell content   |   |  Arbitrary order interspersed with freeblocks.
**       | area           |   |  and free space fragments.
**       |----------------|
**
** HEADER FORMAT (8 or 12 bytes)
** ------------------------------
** Offset  Size  Description
** ------  ----  -----------
** 0       1     Page type (leaf/interior/freelist/etc.)
** 1       2     Offset to first freeblock (0 if none)
** 3       2     Number of cells on this page
** 5       2     Offset to start of cell content area
** 7       1     Number of fragmented free bytes
** 8       4     Right-most child pointer (interior pages only)
**
** PAGE TYPES
** ----------
** - Interior Table (0x02): Non-leaf B-tree nodes for tables
** - Leaf Table (0x05): Leaf B-tree nodes containing actual data
** - Interior Index (0x0A): Non-leaf B-tree nodes for indexes
** - Leaf Index (0x0D): Leaf nodes for indexes
** - Freelist Trunk (0x01): Manages free pages
** - Freelist Leaf (0x03): Contains free page numbers
** - Overflow (0x00): Continuation of large cells
** - Pointer Map (0x04): Used in incremental vacuum
**
** CELL STORAGE
** ------------
** Cells are stored in two parts:
** 1. Cell Pointer Array: Fixed-size 2-byte pointers in sorted key order,
**    growing downward from the header
** 2. Cell Content Area: Variable-size cell data, growing upward from the
**    end of the page
**
** This separation allows:
** - Fast binary search via sorted pointers
** - Efficient space utilization
** - Easy insertion without moving cell data
**
** OPERATIONS
** ----------
** - InsertLeafCell/InsertInteriorCell: Add a new cell maintaining sort order
** - SearchCell: Binary search for a key
** - GetFreeSpace: Calculate available space for new cells
** - SortCells: Reorder cell pointers (cell data remains in place)
** - GetLeafCell/GetInteriorCell: Retrieve and deserialize cell data
**
** SPACE MANAGEMENT
** ----------------
** Free space exists in the middle between the growing cell pointer array
** (top) and cell content area (bottom). The page tracks:
** - CellContentOffset: Where cell content begins
** - NumCells: How many cell pointers exist
** - FragmentedBytes: Wasted space from deleted cells (not fully implemented)
**
 */

/*
Page Layout (4KB = 4096 bytes example):
Offset
0      |----------------|
       | Page Header    |  PageType=Leaf, NumCells=3, CellContentOffset=3900
8      |----------------|
       | Cell Ptr [0]   |------------------+ <- Key=50, Value="Charlie" (46 bytes)
10     | = 3950         |                  |
       |----------------|                  |
       | Cell Ptr [1]   |-------------+    |
12     | = 3900         |             |    |
       |----------------|             |    |
       | Cell Ptr [2]   |--------+    |    |
14     | = 4046         |        |    |    |
       |----------------|        |    |    |
       |                |        |    |    |
       | Unallocated    |        |    |    |
       | Space          |        |    |    |
       |                |        |    |    |
3900   |----------------|        |    |    |
       | Cell 1 Data    |<-------|----+ <- Key=100, Value="Alice" (50 bytes)
       |                |        |         |
3950   |----------------|        |         |
       | Cell 3 Data    |<-------|---------+
       |                |		 |
4046   |----------------|		 |
       | Cell 2 Data    |<-------+ <- Key=200, Value="Bob" (50 bytes)
       |                |
4096   |________________| <- End of page

Key Points:
- Page is 4096 bytes total (0 to 4095)
- Cell pointers SORTED by key: Ptr[0]=key 50, Ptr[1]=key 100, Ptr[2]=key 200
- Cell content in arbitrary order, growing UP from bottom
- Last cell starts at 4046 and goes to 4095 (50 bytes)
*/

import (
	"encoding/binary"
	"errors"
	"sort"
)

const (
	PageTypeInteriorTable PageType = 0x02
	PageTypeLeafTable     PageType = 0x05
	PageTypeInteriorIndex PageType = 0x0A
	PageTypeLeafIndex     PageType = 0x0D
	PageTypeFreelistTrunk PageType = 0x01
	PageTypeFreelistLeaf  PageType = 0x03
	PageTypeOverflow      PageType = 0x00
	PageTypePointerMap    PageType = 0x04
)

type PageHeader struct {
	PageType          PageType
	FirstFreeblock    uint16
	NumCells          uint16
	CellContentOffset uint16
	FragmentedBytes   byte
	RightmostPointer  uint32
}

type Page struct {
	Header PageHeader
	Data   []byte
}

func NewPage(pageType PageType, pageNum uint32) (*Page, error) {
	if PageSize < 512 || (PageSize&(PageSize-1)) != 0 {
		return nil, errors.New("page size must be power of 2 >= 512")
	}

	p := &Page{
		Data: make([]byte, PageSize),
	}

	p.Header = PageHeader{
		PageType:          pageType,
		FirstFreeblock:    0,
		NumCells:          0,
		CellContentOffset: uint16(PageSize),
		FragmentedBytes:   0,
		RightmostPointer:  0,
	}

	p.writeHeader()
	return p, nil
}

func (p *Page) GetHeaderSize() int {
	switch p.Header.PageType {
	case PageTypeInteriorTable, PageTypeInteriorIndex:
		return 12
	case PageTypeLeafTable, PageTypeLeafIndex:
		return 8
	default:
		return 8
	}
}

func (p *Page) writeHeader() {
	h := p.Header

	p.Data[0] = byte(h.PageType)

	binary.BigEndian.PutUint16(p.Data[1:3], h.FirstFreeblock)

	binary.BigEndian.PutUint16(p.Data[3:5], h.NumCells)

	binary.BigEndian.PutUint16(p.Data[5:7], h.CellContentOffset)

	p.Data[7] = h.FragmentedBytes

	if p.Header.PageType == PageTypeInteriorTable || p.Header.PageType == PageTypeInteriorIndex {
		binary.BigEndian.PutUint32(p.Data[8:12], h.RightmostPointer)
	}
}

func (p *Page) readHeader() error {
	if len(p.Data) < 8 {
		return errors.New("page too small to contain header")
	}

	p.Header.PageType = PageType(p.Data[0])

	p.Header.FirstFreeblock = binary.BigEndian.Uint16(p.Data[1:3])

	p.Header.NumCells = binary.BigEndian.Uint16(p.Data[3:5])

	p.Header.CellContentOffset = binary.BigEndian.Uint16(p.Data[5:7])

	p.Header.FragmentedBytes = p.Data[7]

	if p.Header.PageType == PageTypeInteriorTable || p.Header.PageType == PageTypeInteriorIndex {
		if len(p.Data) < 12 {
			return errors.New("page too small for interior page header")
		}
		p.Header.RightmostPointer = binary.BigEndian.Uint32(p.Data[8:12])
	}

	return nil
}

func (p *Page) GetCellPointerArrayOffset() int {
	return p.GetHeaderSize()
}

func (p *Page) GetCellPointer(cellNum uint16) (uint16, error) {
	if cellNum >= p.Header.NumCells {
		return 0, errors.New("cell number out of range")
	}

	offset := p.GetCellPointerArrayOffset() + int(cellNum)*2
	if offset+2 > len(p.Data) {
		return 0, errors.New("invalid cell pointer offset")
	}

	return binary.BigEndian.Uint16(p.Data[offset : offset+2]), nil
}

func (p *Page) SetCellPointer(cellNum uint16, offset uint16) error {
	if cellNum >= p.Header.NumCells {
		return errors.New("cell number out of range")
	}

	ptrOffset := p.GetCellPointerArrayOffset() + int(cellNum)*2
	if ptrOffset+2 > len(p.Data) {
		return errors.New("invalid cell pointer offset")
	}

	binary.BigEndian.PutUint16(p.Data[ptrOffset:ptrOffset+2], offset)
	return nil
}

func (p *Page) GetFreeSpace() uint16 {
	headerSize := uint16(p.GetHeaderSize())
	cellPtrArraySize := p.Header.NumCells * 2
	usedAtStart := headerSize + cellPtrArraySize
	usedAtEnd := uint16(PageSize) - p.Header.CellContentOffset

	if usedAtStart+usedAtEnd >= uint16(PageSize) {
		return 0
	}

	return uint16(PageSize) - usedAtStart - usedAtEnd
}

func (p *Page) CanFit(cellSize uint32) bool {

	requiredSpace := cellSize + 2
	return p.GetFreeSpace() >= uint16(requiredSpace)
}

func (p *Page) InsertLeafCell(cell *LeafCell) error {
	cellData := cell.Serialize()
	cellSize := uint32(len(cellData))

	if !p.CanFit(cellSize) {
		return errors.New("not enough space for cell")
	}

	insertPos := p.findInsertPosition(cell.Key)

	p.Header.CellContentOffset -= uint16(cellSize)
	cellOffset := p.Header.CellContentOffset

	copy(p.Data[cellOffset:cellOffset+uint16(cellSize)], cellData)

	p.insertCellPointer(insertPos, cellOffset)

	p.writeHeader()
	return nil
}

func (p *Page) InsertInteriorCell(cell *InteriorCell) error {
	cellData := cell.Serialize()
	cellSize := uint32(len(cellData))

	if !p.CanFit(cellSize) {
		return errors.New("not enough space for cell")
	}

	insertPos := p.findInsertPosition(cell.Key)

	p.Header.CellContentOffset -= uint16(cellSize)
	cellOffset := p.Header.CellContentOffset

	copy(p.Data[cellOffset:cellOffset+uint16(cellSize)], cellData)

	p.insertCellPointer(insertPos, cellOffset)

	p.writeHeader()
	return nil
}

func (p *Page) findInsertPosition(key uint64) uint16 {
	left := uint16(0)
	right := p.Header.NumCells

	for left < right {
		mid := (left + right) / 2
		cellKey, err := p.GetCellKey(mid)
		if err != nil {
			return left
		}

		if cellKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

func (p *Page) insertCellPointer(position uint16, offset uint16) {
	ptrArrayOffset := p.GetCellPointerArrayOffset()

	if position < p.Header.NumCells {
		srcStart := ptrArrayOffset + int(position)*2
		dstStart := srcStart + 2
		length := int(p.Header.NumCells-position) * 2
		copy(p.Data[dstStart:dstStart+length], p.Data[srcStart:srcStart+length])
	}

	ptrOffset := ptrArrayOffset + int(position)*2
	binary.BigEndian.PutUint16(p.Data[ptrOffset:ptrOffset+2], offset)

	p.Header.NumCells++
}

func (p *Page) GetCellKey(cellNum uint16) (uint64, error) {
	cellOffset, err := p.GetCellPointer(cellNum)
	if err != nil {
		return 0, err
	}

	if int(cellOffset)+8 > len(p.Data) {
		return 0, errors.New("invalid cell offset")
	}

	return binary.BigEndian.Uint64(p.Data[cellOffset : cellOffset+8]), nil
}

func (p *Page) GetLeafCell(cellNum uint16) (*LeafCell, error) {
	if p.Header.PageType != PageTypeLeafTable && p.Header.PageType != PageTypeLeafIndex {
		return nil, errors.New("not a leaf page")
	}

	cellOffset, err := p.GetCellPointer(cellNum)
	if err != nil {
		return nil, err
	}

	if int(cellOffset)+12 > len(p.Data) {
		return nil, errors.New("invalid cell offset")
	}

	valueLen := binary.BigEndian.Uint32(p.Data[cellOffset+8 : cellOffset+12])
	cellSize := 12 + valueLen

	if int(cellOffset)+int(cellSize) > len(p.Data) {
		return nil, errors.New("cell extends beyond page")
	}

	return DeserializeLeafCell(p.Data[cellOffset : cellOffset+uint16(cellSize)])
}

func (p *Page) GetInteriorCell(cellNum uint16) (*InteriorCell, error) {
	if p.Header.PageType != PageTypeInteriorTable && p.Header.PageType != PageTypeInteriorIndex {
		return nil, errors.New("not an interior page")
	}

	cellOffset, err := p.GetCellPointer(cellNum)
	if err != nil {
		return nil, err
	}

	if int(cellOffset)+12 > len(p.Data) {
		return nil, errors.New("invalid cell offset")
	}

	return DeserializeInteriorCell(p.Data[cellOffset : cellOffset+12])
}

func (p *Page) SearchCell(key uint64) (uint16, bool, error) {
	left := uint16(0)
	right := p.Header.NumCells

	for left < right {
		mid := (left + right) / 2
		cellKey, err := p.GetCellKey(mid)
		if err != nil {
			return 0, false, err
		}

		if cellKey == key {
			return mid, true, nil
		} else if cellKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left, false, nil
}

func (p *Page) GetAllCellKeys() ([]uint64, error) {
	keys := make([]uint64, p.Header.NumCells)
	for i := uint16(0); i < p.Header.NumCells; i++ {
		key, err := p.GetCellKey(i)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}

func (p *Page) SortCells() error {
	if p.Header.NumCells == 0 {
		return nil
	}

	type cellPtr struct {
		key    uint64
		offset uint16
	}
	pairs := make([]cellPtr, p.Header.NumCells)

	for i := uint16(0); i < p.Header.NumCells; i++ {
		key, err := p.GetCellKey(i)
		if err != nil {
			return err
		}
		ptr, err := p.GetCellPointer(i)
		if err != nil {
			return err
		}
		pairs[i] = cellPtr{key: key, offset: ptr}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].key < pairs[j].key
	})

	for i, pair := range pairs {
		if err := p.SetCellPointer(uint16(i), pair.offset); err != nil {
			return err
		}
	}

	return nil
}

func (p *Page) deleteCell(cellNum uint16) error {
	if cellNum >= p.Header.NumCells {
		return errors.New("cell number out of range")
	}

	ptrArrayOffset := p.GetCellPointerArrayOffset()

	if cellNum < p.Header.NumCells-1 {
		srcStart := ptrArrayOffset + int(cellNum+1)*2
		dstStart := ptrArrayOffset + int(cellNum)*2
		length := int(p.Header.NumCells-cellNum-1) * 2
		copy(p.Data[dstStart:dstStart+length], p.Data[srcStart:srcStart+length])
	}

	p.Header.NumCells--

	p.writeHeader()

	return nil
}
