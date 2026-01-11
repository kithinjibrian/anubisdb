package storage

import (
	"errors"
)

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

func (it *Iterator) Next() (uint64, []byte, error) {
	if it.done {
		return 0, nil, errors.New("iterator exhausted")
	}

	page, err := it.btree.pager.ReadPage(it.currentPage)
	if err != nil {
		return 0, nil, err
	}

	if it.currentCell >= page.Header.NumCells {

		nextPage, err := it.findNextLeafPage(page)

		if err != nil {
			it.done = true
			return 0, nil, errors.New("no more entries")
		}

		it.currentPage = nextPage
		it.currentCell = 0

		page, err = it.btree.pager.ReadPage(it.currentPage)
		if err != nil {
			return 0, nil, err
		}
	}

	cell, err := page.GetLeafCell(it.currentCell)
	if err != nil {
		return 0, nil, err
	}

	it.currentCell++

	return cell.Key, cell.Value, nil
}

func (it *Iterator) HasNext() bool {
	if it.done {
		return false
	}

	page, err := it.btree.pager.ReadPage(it.currentPage)
	if err != nil {
		return false
	}

	if it.currentCell < page.Header.NumCells {
		return true
	}

	_, err = it.findNextLeafPage(page)

	return err == nil
}

func (it *Iterator) findNextLeafPage(currentPage *Page) (uint32, error) {

	if currentPage.Header.NumCells == 0 {
		return 0, errors.New("empty page")
	}

	lastKey, err := currentPage.GetCellKey(currentPage.Header.NumCells - 1)
	if err != nil {
		return 0, err
	}

	return it.btree.findLeafPageForNextKey(lastKey + 1)
}
