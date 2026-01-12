package storage

import "errors"

type Iterator struct {
	btree       *BTree
	currentPage uint32
	currentCell uint16
	done        bool
}

func (bt *BTree) NewIterator() (*Iterator, error) {
	leftmostPage, err := bt.findLeftmostLeaf()
	if err != nil {
		return nil, err
	}

	return &Iterator{
		btree:       bt,
		currentPage: leftmostPage,
		currentCell: 0,
		done:        false,
	}, nil
}

func (bt *BTree) findLeftmostLeaf() (uint32, error) {
	currentPageNum := bt.rootPage

	for {
		page, err := bt.pager.ReadPage(currentPageNum)
		if err != nil {
			return 0, err
		}

		if isLeafPage(page) {
			return currentPageNum, nil
		}

		if page.Header.NumCells == 0 {
			currentPageNum = page.Header.RightmostPointer
		} else {
			cell, err := page.GetInteriorCell(0)
			if err != nil {
				return 0, err
			}
			currentPageNum = cell.ChildPage
		}
	}
}

func (it *Iterator) HasNext() bool {
	if it.done {
		return false
	}

	page, err := it.btree.pager.ReadPage(it.currentPage)
	if err != nil {
		it.done = true
		return false
	}

	return it.currentCell < page.Header.NumCells
}

func (it *Iterator) Next() (Key, []byte, error) {
	if !it.HasNext() {
		return nil, nil, errors.New("no more entries")
	}

	page, err := it.btree.pager.ReadPage(it.currentPage)
	if err != nil {
		it.done = true
		return nil, nil, err
	}

	cell, err := page.GetLeafCell(it.currentCell)
	if err != nil {
		it.done = true
		return nil, nil, err
	}

	key := cell.Key
	value := cell.Value

	it.currentCell++

	if it.currentCell >= page.Header.NumCells {
		it.done = true
	}

	return key, value, nil
}
