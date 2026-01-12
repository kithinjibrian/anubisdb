package storage

import (
	"encoding/binary"
	"errors"
	"os"
)

var (
	dbMagicNumber = [8]byte{'A', 'n', 'u', 'b', 'i', 's', 'D', 'B'}
)

type DatabaseHeader struct {
	MagicNumber [8]byte
	Version     uint32
	Reserved    [PageSize - 12]byte
}

type Pager struct {
	file     *os.File
	numPages uint32
	header   DatabaseHeader
}

func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	p := &Pager{file: file}

	if stat.Size() == 0 {
		p.header = DatabaseHeader{
			MagicNumber: dbMagicNumber,
			Version:     1,
		}
		if err := p.writeHeader(); err != nil {
			file.Close()
			return nil, err
		}
		p.numPages = 0
	} else {
		if stat.Size()%PageSize != 0 {
			file.Close()
			return nil, errors.New("corrupted database file: size not multiple of page size")
		}

		if err := p.readHeader(); err != nil {
			file.Close()
			return nil, err
		}

		totalPages := uint32(stat.Size()) / PageSize
		if totalPages > 0 {
			p.numPages = totalPages - 1
		} else {
			p.numPages = 0
		}
	}

	return p, nil
}

func (p *Pager) readHeader() error {
	buf := make([]byte, PageSize)
	_, err := p.file.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	copy(p.header.MagicNumber[:], buf[0:8])
	p.header.Version = binary.BigEndian.Uint32(buf[8:12])
	copy(p.header.Reserved[:], buf[12:PageSize])

	if p.header.MagicNumber != dbMagicNumber {
		return errors.New("invalid database file: bad magic number")
	}

	return nil
}

func (p *Pager) writeHeader() error {
	buf := make([]byte, PageSize)
	copy(buf[0:8], p.header.MagicNumber[:])
	binary.BigEndian.PutUint32(buf[8:12], p.header.Version)
	copy(buf[12:PageSize], p.header.Reserved[:])

	_, err := p.file.WriteAt(buf, 0)
	return err
}

func (p *Pager) Close() error {
	return p.file.Close()
}

func (p *Pager) ReadPage(pageNum uint32) (*Page, error) {

	if pageNum == 0 {
		return nil, errors.New("page 0 is reserved for database header")
	}

	if pageNum > p.numPages {
		return nil, errors.New("page number out of range")
	}

	page := &Page{
		Data: make([]byte, PageSize),
	}

	offset := int64(PageSize) * int64(pageNum)
	_, err := p.file.ReadAt(page.Data, offset)
	if err != nil {
		return nil, err
	}

	if err := page.readHeader(); err != nil {
		return nil, err
	}

	return page, nil
}

func (p *Pager) WritePage(pageNum uint32, page *Page) error {

	if pageNum == 0 {
		return errors.New("page 0 is reserved for database header")
	}

	if pageNum > p.numPages {
		return errors.New("page number out of range")
	}

	page.writeHeader()

	offset := int64(PageSize) * int64(pageNum)
	_, err := p.file.WriteAt(page.Data, offset)
	return err
}

func (p *Pager) AllocatePage(pageType PageType, parent uint32) (uint32, *Page, error) {

	pageNum := p.numPages + 1

	page, err := NewPage(pageType, pageNum)
	if err != nil {
		return 0, nil, err
	}

	page.Header.ParentPage = parent
	page.writeHeader()

	offset := int64(PageSize) * int64(pageNum)
	_, err = p.file.WriteAt(page.Data, offset)
	if err != nil {
		return 0, nil, err
	}

	p.numPages++
	return pageNum, page, nil
}

func (p *Pager) ReadOrAllocatePage(pageNum uint32, pageType PageType, parent uint32) (*Page, error) {
	if pageNum > 0 && pageNum <= p.GetNumPages() {
		return p.ReadPage(pageNum)
	}

	_, page, err := p.AllocatePage(pageType, parent)
	return page, err
}

func (p *Pager) GetNumPages() uint32 {
	return p.numPages
}

func (p *Pager) Sync() error {
	return p.file.Sync()
}

func (p *Pager) GetHeader() DatabaseHeader {
	return p.header
}
