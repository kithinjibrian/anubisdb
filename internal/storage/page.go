package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
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

const (
	MinFreeSpaceThreshold = 64
)

type PageHeader struct {
	PageType          PageType
	FirstFreeblock    uint16
	NumCells          uint16
	CellContentOffset uint16
	FragmentedBytes   byte

	RightmostPointer uint32

	ParentPage uint32
	NextLeaf   uint32
	PrevLeaf   uint32
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
		ParentPage:        0,
		NextLeaf:          0,
		PrevLeaf:          0,
	}

	p.writeHeader()
	return p, nil
}

func isLeaf(t PageType) bool {
	return t == PageTypeLeafTable || t == PageTypeLeafIndex
}

func isInterior(t PageType) bool {
	return t == PageTypeInteriorTable || t == PageTypeInteriorIndex
}

func (p *Page) GetHeaderSize() int {
	switch p.Header.PageType {
	case PageTypeInteriorTable, PageTypeInteriorIndex:
		return 16
	case PageTypeLeafTable, PageTypeLeafIndex:
		return 20
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

	if isInterior(h.PageType) {
		binary.BigEndian.PutUint32(p.Data[8:12], h.RightmostPointer)
		binary.BigEndian.PutUint32(p.Data[12:16], h.ParentPage)
	} else if isLeaf(h.PageType) {
		binary.BigEndian.PutUint32(p.Data[8:12], h.ParentPage)
		binary.BigEndian.PutUint32(p.Data[12:16], h.NextLeaf)
		binary.BigEndian.PutUint32(p.Data[16:20], h.PrevLeaf)
	}
}

func (p *Page) readHeader() error {
	if len(p.Data) < 8 {
		return errors.New("page too small")
	}

	p.Header.PageType = PageType(p.Data[0])
	p.Header.FirstFreeblock = binary.BigEndian.Uint16(p.Data[1:3])
	p.Header.NumCells = binary.BigEndian.Uint16(p.Data[3:5])
	p.Header.CellContentOffset = binary.BigEndian.Uint16(p.Data[5:7])
	p.Header.FragmentedBytes = p.Data[7]

	if isInterior(p.Header.PageType) {
		if len(p.Data) < 16 {
			return errors.New("page too small for interior header")
		}
		p.Header.RightmostPointer = binary.BigEndian.Uint32(p.Data[8:12])
		p.Header.ParentPage = binary.BigEndian.Uint32(p.Data[12:16])
	} else if isLeaf(p.Header.PageType) {
		if len(p.Data) < 20 {
			return errors.New("page too small for leaf header")
		}
		p.Header.ParentPage = binary.BigEndian.Uint32(p.Data[8:12])
		p.Header.NextLeaf = binary.BigEndian.Uint32(p.Data[12:16])
		p.Header.PrevLeaf = binary.BigEndian.Uint32(p.Data[16:20])
	}

	if err := p.validateHeader(); err != nil {
		return fmt.Errorf("invalid page header: %w", err)
	}

	return nil
}

func (p *Page) validateHeader() error {
	headerSize := uint16(p.GetHeaderSize())

	if p.Header.CellContentOffset < headerSize || p.Header.CellContentOffset > uint16(PageSize) {
		return fmt.Errorf("invalid CellContentOffset: %d", p.Header.CellContentOffset)
	}

	minPtrArrayEnd := headerSize + (p.Header.NumCells * 2)
	if minPtrArrayEnd > p.Header.CellContentOffset {
		return errors.New("cell pointer array overlaps with cell content area")
	}

	return nil
}

func (p *Page) GetCellPointerArrayOffset() int {
	return p.GetHeaderSize()
}

func (p *Page) GetCellPointer(cellNum uint16) (uint16, error) {
	if cellNum >= p.Header.NumCells {
		return 0, fmt.Errorf("cell number %d out of range (numCells=%d)", cellNum, p.Header.NumCells)
	}
	offset := p.GetCellPointerArrayOffset() + int(cellNum)*2

	if offset+2 > len(p.Data) {
		return 0, errors.New("cell pointer offset exceeds page size")
	}

	return binary.BigEndian.Uint16(p.Data[offset : offset+2]), nil
}

func (p *Page) SetCellPointer(cellNum uint16, offset uint16) error {
	if cellNum >= p.Header.NumCells {
		return fmt.Errorf("cell number %d out of range (numCells=%d)", cellNum, p.Header.NumCells)
	}

	ptr := p.GetCellPointerArrayOffset() + int(cellNum)*2
	if ptr+2 > len(p.Data) {
		return errors.New("cell pointer offset exceeds page size")
	}

	if offset < uint16(p.GetHeaderSize()) || offset >= uint16(PageSize) {
		return fmt.Errorf("invalid cell offset: %d", offset)
	}

	binary.BigEndian.PutUint16(p.Data[ptr:ptr+2], offset)
	return nil
}

func (p *Page) GetFreeSpace() uint16 {
	headerSize := uint16(p.GetHeaderSize())
	ptrArraySize := p.Header.NumCells * 2
	usedStart := headerSize + ptrArraySize

	if p.Header.CellContentOffset < usedStart {
		return 0
	}

	freeSpace := p.Header.CellContentOffset - usedStart
	return freeSpace
}

func (p *Page) GetTotalFreeSpace() uint16 {
	return p.GetFreeSpace() + uint16(p.Header.FragmentedBytes)
}

func (p *Page) CanFit(cellSize uint32) bool {

	required := uint16(cellSize) + 2

	if p.GetFreeSpace() >= required {
		return true
	}

	if p.GetTotalFreeSpace() >= required {
		return true
	}

	return false
}

func (p *Page) InsertLeafCell(cell *LeafCell) error {
	return p.insertCell(cell.Key, cell.Serialize())
}

func (p *Page) InsertInteriorCell(cell *InteriorCell) error {
	return p.insertCell(cell.Key, cell.Serialize())
}

func (p *Page) insertCell(key Key, data []byte) error {
	cellSize := uint32(len(data))

	if !p.CanFit(cellSize) {
		return errors.New("not enough space")
	}

	if p.GetFreeSpace() < uint16(cellSize)+2 && p.GetTotalFreeSpace() >= uint16(cellSize)+2 {
		if err := p.Defragment(); err != nil {
			return fmt.Errorf("defragmentation failed: %w", err)
		}
	}

	if p.GetFreeSpace() < uint16(cellSize)+2 {
		return errors.New("not enough space after defragmentation")
	}

	newOffset := p.Header.CellContentOffset - uint16(cellSize)
	headerSize := uint16(p.GetHeaderSize())
	ptrArrayEnd := headerSize + (p.Header.NumCells+1)*2

	if newOffset < ptrArrayEnd {
		return errors.New("cell content would overlap with pointer array")
	}

	pos := p.findInsertPosition(key)
	p.Header.CellContentOffset = newOffset
	offset := p.Header.CellContentOffset

	if int(offset)+len(data) > len(p.Data) {
		return errors.New("cell data exceeds page size")
	}

	copy(p.Data[offset:], data)

	if err := p.insertCellPointer(pos, offset); err != nil {
		return err
	}

	p.writeHeader()
	return nil
}

func (p *Page) findInsertPosition(key Key) uint16 {
	l, r := uint16(0), p.Header.NumCells
	for l < r {
		m := (l + r) / 2
		k, err := p.GetCellKey(m)
		if err != nil {

			return l
		}
		if k.Compare(key) < 0 {
			l = m + 1
		} else {
			r = m
		}
	}
	return l
}

func (p *Page) insertCellPointer(pos uint16, offset uint16) error {

	headerSize := uint16(p.GetHeaderSize())
	newPtrArrayEnd := headerSize + (p.Header.NumCells+1)*2

	if newPtrArrayEnd > p.Header.CellContentOffset {
		return errors.New("no room for cell pointer")
	}

	if pos > p.Header.NumCells {
		return fmt.Errorf("invalid insert position: %d", pos)
	}

	base := p.GetCellPointerArrayOffset()

	srcStart := base + int(pos)*2
	srcEnd := base + int(p.Header.NumCells)*2
	dstStart := base + int(pos+1)*2

	if srcEnd > srcStart && dstStart+2 <= len(p.Data) {
		copy(p.Data[dstStart:], p.Data[srcStart:srcEnd])
	}

	if base+int(pos)*2+2 > len(p.Data) {
		return errors.New("pointer offset exceeds page size")
	}

	binary.BigEndian.PutUint16(p.Data[base+int(pos)*2:], offset)
	p.Header.NumCells++

	return nil
}

func (p *Page) GetCellKey(cellNum uint16) (Key, error) {
	offset, err := p.GetCellPointer(cellNum)
	if err != nil {
		return nil, err
	}

	if int(offset)+4 > len(p.Data) {
		return nil, errors.New("key length field exceeds page size")
	}

	keyLen := binary.BigEndian.Uint32(p.Data[offset : offset+4])

	if int(offset)+4+int(keyLen) > len(p.Data) {
		return nil, fmt.Errorf("key data exceeds page size (offset=%d, keyLen=%d)", offset, keyLen)
	}

	return DecodeKey(p.Data[offset+4 : offset+4+uint16(keyLen)])
}

func (p *Page) GetLeafCell(cellNum uint16) (*LeafCell, error) {
	offset, err := p.GetCellPointer(cellNum)
	if err != nil {
		return nil, err
	}

	if int(offset) >= len(p.Data) {
		return nil, errors.New("cell offset exceeds page size")
	}

	return DeserializeLeafCell(p.Data[offset:])
}

func (p *Page) GetInteriorCell(cellNum uint16) (*InteriorCell, error) {
	offset, err := p.GetCellPointer(cellNum)
	if err != nil {
		return nil, err
	}

	if int(offset) >= len(p.Data) {
		return nil, errors.New("cell offset exceeds page size")
	}

	return DeserializeInteriorCell(p.Data[offset:])
}

func (p *Page) SearchCell(key Key) (uint16, bool, error) {
	l, r := uint16(0), p.Header.NumCells
	for l < r {
		m := (l + r) / 2
		k, err := p.GetCellKey(m)
		if err != nil {
			return 0, false, err
		}

		switch k.Compare(key) {
		case 0:
			return m, true, nil
		case -1:
			l = m + 1
		default:
			r = m
		}
	}
	return l, false, nil
}

func (p *Page) GetAllCellKeys() ([]Key, error) {
	keys := make([]Key, p.Header.NumCells)
	for i := range keys {
		key, err := p.GetCellKey(uint16(i))
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}

func (p *Page) SortCells() error {
	type pair struct {
		key    Key
		offset uint16
	}

	ps := make([]pair, p.Header.NumCells)
	for i := range ps {
		key, err := p.GetCellKey(uint16(i))
		if err != nil {
			return err
		}
		offset, err := p.GetCellPointer(uint16(i))
		if err != nil {
			return err
		}
		ps[i].key = key
		ps[i].offset = offset
	}

	sort.Slice(ps, func(i, j int) bool {
		return ps[i].key.Compare(ps[j].key) < 0
	})

	for i, pr := range ps {
		if err := p.SetCellPointer(uint16(i), pr.offset); err != nil {
			return err
		}
	}

	return nil
}

func (p *Page) deleteCell(cellNum uint16) error {
	if cellNum >= p.Header.NumCells {
		return fmt.Errorf("cell number %d out of range", cellNum)
	}

	offset, err := p.GetCellPointer(cellNum)
	if err != nil {
		return err
	}

	var cellSize uint16
	if isLeaf(p.Header.PageType) {
		cell, err := p.GetLeafCell(cellNum)
		if err == nil {
			cellSize = uint16(cell.Size())
		}
	} else {
		cell, err := p.GetInteriorCell(cellNum)
		if err == nil {
			cellSize = uint16(cell.Size())
		}
	}

	if cellSize > 0 && int(offset)+int(cellSize) <= len(p.Data) {
		for i := uint16(0); i < cellSize; i++ {
			p.Data[offset+i] = 0
		}
	}

	if cellSize > 0 {
		newFragmented := uint16(p.Header.FragmentedBytes) + cellSize
		if newFragmented > 255 {
			p.Header.FragmentedBytes = 255
		} else {
			p.Header.FragmentedBytes = byte(newFragmented)
		}
	}

	ptrs := p.GetCellPointerArrayOffset()
	srcStart := ptrs + int(cellNum+1)*2
	srcEnd := ptrs + int(p.Header.NumCells)*2
	dstStart := ptrs + int(cellNum)*2

	if srcEnd > srcStart {
		copy(p.Data[dstStart:], p.Data[srcStart:srcEnd])
	}

	lastPtrOffset := ptrs + int(p.Header.NumCells-1)*2
	if lastPtrOffset+2 <= len(p.Data) {
		p.Data[lastPtrOffset] = 0
		p.Data[lastPtrOffset+1] = 0
	}

	p.Header.NumCells--
	p.writeHeader()

	if p.Header.FragmentedBytes > MinFreeSpaceThreshold {
		_ = p.Defragment()
	}

	return nil
}

func (p *Page) Defragment() error {
	if p.Header.NumCells == 0 {

		p.Header.CellContentOffset = uint16(PageSize)
		p.Header.FragmentedBytes = 0
		p.writeHeader()
		return nil
	}

	type cellInfo struct {
		offset uint16
		data   []byte
	}

	cells := make([]cellInfo, p.Header.NumCells)

	for i := uint16(0); i < p.Header.NumCells; i++ {
		offset, err := p.GetCellPointer(i)
		if err != nil {
			return err
		}

		var cellData []byte
		if isLeaf(p.Header.PageType) {
			cell, err := p.GetLeafCell(i)
			if err != nil {
				return err
			}
			cellData = cell.Serialize()
		} else {
			cell, err := p.GetInteriorCell(i)
			if err != nil {
				return err
			}
			cellData = cell.Serialize()
		}

		cells[i] = cellInfo{
			offset: offset,
			data:   cellData,
		}
	}

	newContentOffset := uint16(PageSize)

	for i := uint16(0); i < p.Header.NumCells; i++ {
		cellSize := uint16(len(cells[i].data))
		newContentOffset -= cellSize

		copy(p.Data[newContentOffset:], cells[i].data)

		if err := p.SetCellPointer(i, newContentOffset); err != nil {
			return err
		}
	}

	p.Header.CellContentOffset = newContentOffset
	p.Header.FragmentedBytes = 0
	p.writeHeader()

	return nil
}

func (p *Page) GetCellSize(cellNum uint16) (uint16, error) {
	if isLeaf(p.Header.PageType) {
		cell, err := p.GetLeafCell(cellNum)
		if err != nil {
			return 0, err
		}
		return uint16(cell.Size()), nil
	} else {
		cell, err := p.GetInteriorCell(cellNum)
		if err != nil {
			return 0, err
		}
		return uint16(cell.Size()), nil
	}
}
