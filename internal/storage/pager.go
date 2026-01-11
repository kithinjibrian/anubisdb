package storage

/*
The Pager manages the database file and handles reading/writing pages to disk.
It acts as the interface between the in-memory B-Tree structures and persistent
storage on disk.

FILE STRUCTURE
--------------
The database file is organized as follows:

  |---------------------|
  | Page 0: DB Header   |  Special page containing metadata
  |---------------------|
  | Page 1              |  First data page (B-tree pages, etc.)
  |---------------------|
  | Page 2              |
  |---------------------|
  | ...                 |
  |---------------------|
  | Page N              |  Last allocated page
  |---------------------|

DATABASE HEADER (Page 0)
------------------------
The first page (offset 0) contains database metadata:

  Offset  Size    Description
  ------  ----    -----------
  0       8       Magic number: "AnubisDB" (file type identifier)
  8       4       Version number (currently 1)
  12      N       Reserved space (rest of page, for future use)

This header helps:
- Verify the file is a valid database file
- Check compatibility (version number)
- Reserve space for future metadata

PAGE NUMBERING
--------------
Important: Page numbering vs. file offsets are different!

- Page 0 = Database header (offset 0)
- Page 1 = First data page (offset PageSize * 1)
- Page 2 = Second data page (offset PageSize * 2)
- Page N = Nth data page (offset PageSize * (N+1))

When the B-Tree refers to "page 0", it means the first data page,
which is actually at file offset PageSize (after the header).

FILE OFFSET CALCULATION
-----------------------
For page number N:
  file_offset = PageSize * (N + 1)

Examples (assuming PageSize = 4096):
  Page 0 (header) → offset 0
  Page 0 (data)   → offset 4096
  Page 1 (data)   → offset 8192
  Page 5 (data)   → offset 24576
*/

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
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
	mu       sync.RWMutex
}

func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	p := &Pager{file: file}

	if stat.Size() == 0 {
		p.header = DatabaseHeader{
			MagicNumber: [8]byte{'A', 'n', 'u', 'b', 'i', 's', 'D', 'B'},
			Version:     1,
		}
		if err := p.writeHeader(); err != nil {
			return nil, err
		}
		p.numPages = 0
	} else {
		if err := p.readHeader(); err != nil {
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

	expected := [8]byte{'A', 'n', 'u', 'b', 'i', 's', 'D', 'B'}
	if p.header.MagicNumber != expected {
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
	p.mu.RLock()
	defer p.mu.RUnlock()

	if pageNum >= p.numPages {
		return nil, errors.New("page number out of range")
	}

	page := &Page{
		Data: make([]byte, PageSize),
	}

	offset := PageSize * (int64(pageNum) + 1)
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
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageNum >= p.numPages {
		return errors.New("page number out of range")
	}

	page.writeHeader()

	offset := PageSize * (int64(pageNum) + 1)
	_, err := p.file.WriteAt(page.Data, offset)
	return err
}

func (p *Pager) AllocatePage(pageType PageType, tableID uint32) (uint32, *Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pageNum := p.numPages

	page, err := NewPage(pageType, tableID)
	if err != nil {
		return 0, nil, err
	}

	offset := PageSize * (int64(pageNum) + 1)
	_, err = p.file.WriteAt(page.Data, offset)
	if err != nil {
		return 0, nil, err
	}

	p.numPages++

	return pageNum, page, nil
}

func (p *Pager) GetNumPages() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numPages
}

func (p *Pager) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Sync()
}
