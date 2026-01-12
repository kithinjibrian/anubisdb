package storage

import "errors"

type Iterator struct {
	tree        *BTree
	currentPage uint32
	currentIdx  uint16
}

func (tree *BTree) NewIterator() (*Iterator, error) {
	leftmost, err := tree.findLeftmostLeaf()
	if err != nil {
		return nil, err
	}

	return &Iterator{
		tree:        tree,
		currentPage: leftmost,
		currentIdx:  0,
	}, nil
}

func (it *Iterator) HasNext() bool {
	if it.currentPage == 0 {
		return false
	}

	page, err := it.tree.pager.ReadPage(it.currentPage)
	if err != nil {
		return false
	}

	return it.currentIdx < page.Header.NumCells || page.Header.NextLeaf != 0
}

func (it *Iterator) Next() (Key, []byte, error) {
	page, err := it.tree.pager.ReadPage(it.currentPage)
	if err != nil {
		return nil, nil, err
	}

	if it.currentIdx >= page.Header.NumCells {
		if page.Header.NextLeaf == 0 {
			return nil, nil, errors.New("no more entries")
		}
		it.currentPage = page.Header.NextLeaf
		it.currentIdx = 0
		page, err = it.tree.pager.ReadPage(it.currentPage)
		if err != nil {
			return nil, nil, err
		}
	}

	cell, err := page.GetLeafCell(it.currentIdx)
	if err != nil {
		return nil, nil, err
	}

	it.currentIdx++
	return cell.Key, cell.Value, nil
}
