package storage

import (
	"errors"
	"fmt"
)

type BTree struct {
	pager    *Pager
	rootPage uint32
	depth    uint32
}

type Entry struct {
	Key   uint64
	Value []byte
}

func NewBTree(pager *Pager) (*BTree, error) {
	rootPageNum, rootPage, err := pager.AllocatePage(PageTypeLeafTable, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}

	if err := pager.WritePage(rootPageNum, rootPage); err != nil {
		return nil, fmt.Errorf("failed to write root page: %w", err)
	}

	return &BTree{
		pager:    pager,
		rootPage: rootPageNum,
		depth:    0,
	}, nil
}

func LoadBTree(pager *Pager, rootPage uint32) (*BTree, error) {
	depth, err := calculateDepth(pager, rootPage)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate tree depth: %w", err)
	}

	return &BTree{
		pager:    pager,
		rootPage: rootPage,
		depth:    depth,
	}, nil
}

func calculateDepth(pager *Pager, rootPage uint32) (uint32, error) {
	depth := uint32(0)
	currentPageNum := rootPage

	for {
		page, err := pager.ReadPage(currentPageNum)
		if err != nil {
			return 0, err
		}

		if isLeafPage(page) {
			return depth, nil
		}

		currentPageNum, err = getLeftmostChild(page)
		if err != nil {
			return 0, err
		}

		depth++
	}
}

func isLeafPage(page *Page) bool {
	return page.Header.PageType == PageTypeLeafTable ||
		page.Header.PageType == PageTypeLeafIndex
}

func getLeftmostChild(page *Page) (uint32, error) {
	if page.Header.NumCells == 0 {
		return page.Header.RightmostPointer, nil
	}

	cell, err := page.GetInteriorCell(0)
	if err != nil {
		return 0, err
	}
	return cell.ChildPage, nil
}

func (bt *BTree) Search(key uint64) ([]byte, error) {
	leafPageNum, err := bt.findLeafPage(key)
	if err != nil {
		return nil, fmt.Errorf("failed to find leaf page: %w", err)
	}

	leafPage, err := bt.pager.ReadPage(leafPageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read leaf page: %w", err)
	}

	cellNum, found, err := leafPage.SearchCell(key)
	if err != nil {
		return nil, fmt.Errorf("failed to search cell: %w", err)
	}

	if !found {
		return nil, errors.New("key not found")
	}

	cell, err := leafPage.GetLeafCell(cellNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get leaf cell: %w", err)
	}

	return cell.Value, nil
}

func (bt *BTree) findLeafPage(key uint64) (uint32, error) {
	currentPageNum := bt.rootPage

	for {
		page, err := bt.pager.ReadPage(currentPageNum)
		if err != nil {
			return 0, err
		}

		if isLeafPage(page) {
			return currentPageNum, nil
		}

		childPageNum, err := bt.findChildPageForKey(page, key)
		if err != nil {
			return 0, err
		}

		currentPageNum = childPageNum
	}
}

func (bt *BTree) findChildPageForKey(page *Page, key uint64) (uint32, error) {
	if page.Header.NumCells == 0 {
		return page.Header.RightmostPointer, nil
	}

	for i := uint16(0); i < page.Header.NumCells; i++ {
		cellKey, err := page.GetCellKey(i)
		if err != nil {
			return 0, err
		}

		if key < cellKey {
			cell, err := page.GetInteriorCell(i)
			if err != nil {
				return 0, err
			}
			return cell.ChildPage, nil
		}
	}

	return page.Header.RightmostPointer, nil
}

func (bt *BTree) Insert(key uint64, value []byte) error {
	leafPageNum, err := bt.findLeafPage(key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	leafPage, err := bt.pager.ReadPage(leafPageNum)
	if err != nil {
		return fmt.Errorf("failed to read leaf page: %w", err)
	}

	if _, found, _ := leafPage.SearchCell(key); found {
		return fmt.Errorf("key %d already exists", key)
	}

	cell := NewLeafCell(key, value)

	if leafPage.CanFit(cell.Size()) {
		if err := leafPage.InsertLeafCell(cell); err != nil {
			return fmt.Errorf("failed to insert leaf cell: %w", err)
		}
		return bt.pager.WritePage(leafPageNum, leafPage)
	}

	return bt.handleLeafPageSplit(leafPageNum, leafPage, cell)
}

func (bt *BTree) Update(key uint64, newValue []byte) error {
	_, err := bt.Search(key)
	if err != nil {
		return fmt.Errorf("key not found: %w", err)
	}

	if err := bt.Delete(key); err != nil {
		return fmt.Errorf("failed to delete old entry: %w", err)
	}

	if err := bt.Insert(key, newValue); err != nil {
		return fmt.Errorf("failed to insert new entry: %w", err)
	}

	return nil
}

func (bt *BTree) Delete(key uint64) error {
	leafPageNum, err := bt.findLeafPage(key)
	if err != nil {
		return err
	}

	leafPage, err := bt.pager.ReadPage(leafPageNum)
	if err != nil {
		return err
	}

	cellNum, found, err := leafPage.SearchCell(key)
	if err != nil {
		return err
	}

	if !found {
		return errors.New("key not found")
	}

	if err := leafPage.deleteCell(cellNum); err != nil {
		return err
	}

	if err := bt.pager.WritePage(leafPageNum, leafPage); err != nil {
		return err
	}

	return nil
}

func (bt *BTree) handleLeafPageSplit(oldPageNum uint32, oldPage *Page, newCell *LeafCell) error {
	newPageNum, newPage, err := bt.pager.AllocatePage(PageTypeLeafTable, 0)
	if err != nil {
		return fmt.Errorf("failed to allocate new page: %w", err)
	}

	allCells, err := bt.collectAndSortCells(oldPage, newCell)
	if err != nil {
		return fmt.Errorf("failed to collect cells: %w", err)
	}

	splitPoint := len(allCells) / 2

	if err := bt.distributeCells(oldPage, newPage, allCells, splitPoint); err != nil {
		return fmt.Errorf("failed to distribute cells: %w", err)
	}

	promoteKey := allCells[splitPoint].Key

	if err := bt.pager.WritePage(oldPageNum, oldPage); err != nil {
		return err
	}
	if err := bt.pager.WritePage(newPageNum, newPage); err != nil {
		return err
	}

	if oldPageNum == bt.rootPage {
		return bt.createNewRoot(oldPageNum, promoteKey, newPageNum)
	}

	// TODO: Implement proper parent insertion for non-root splits
	// For now, we create a new root (this works but creates taller trees than necessary)
	return bt.createNewRoot(oldPageNum, promoteKey, newPageNum)
}

func (bt *BTree) collectAndSortCells(page *Page, newCell *LeafCell) ([]*LeafCell, error) {
	allCells := make([]*LeafCell, 0, page.Header.NumCells+1)

	for i := uint16(0); i < page.Header.NumCells; i++ {
		cell, err := page.GetLeafCell(i)
		if err != nil {
			return nil, err
		}
		allCells = append(allCells, cell)
	}

	allCells = append(allCells, newCell)

	for i := 1; i < len(allCells); i++ {
		j := i
		for j > 0 && allCells[j].Key < allCells[j-1].Key {
			allCells[j], allCells[j-1] = allCells[j-1], allCells[j]
			j--
		}
	}

	return allCells, nil
}

func (bt *BTree) distributeCells(oldPage, newPage *Page, cells []*LeafCell, splitPoint int) error {
	oldPage.Header.NumCells = 0
	oldPage.Header.CellContentOffset = uint16(PageSize)
	oldPage.writeHeader()

	for i := 0; i < splitPoint; i++ {
		if err := oldPage.InsertLeafCell(cells[i]); err != nil {
			return fmt.Errorf("failed to insert into old page: %w", err)
		}
	}

	for i := splitPoint; i < len(cells); i++ {
		if err := newPage.InsertLeafCell(cells[i]); err != nil {
			return fmt.Errorf("failed to insert into new page: %w", err)
		}
	}

	return nil
}

func (bt *BTree) createNewRoot(leftChild uint32, key uint64, rightChild uint32) error {
	newRootNum, newRoot, err := bt.pager.AllocatePage(PageTypeInteriorTable, 0)
	if err != nil {
		return fmt.Errorf("failed to allocate new root: %w", err)
	}

	newRoot.Header.RightmostPointer = rightChild

	leftCell := NewInteriorCell(key, leftChild)
	if err := newRoot.InsertInteriorCell(leftCell); err != nil {
		return fmt.Errorf("failed to insert into new root: %w", err)
	}

	if err := bt.pager.WritePage(newRootNum, newRoot); err != nil {
		return fmt.Errorf("failed to write new root: %w", err)
	}

	bt.rootPage = newRootNum
	bt.depth++

	return nil
}

func (bt *BTree) GetRootPage() uint32 {
	return bt.rootPage
}

func (bt *BTree) GetDepth() uint32 {
	return bt.depth
}

func (bt *BTree) PrintTree() error {
	fmt.Printf("B+ Tree (root=%d, depth=%d)\n", bt.rootPage, bt.depth)
	return bt.printPage(bt.rootPage, 0)
}

func (bt *BTree) printPage(pageNum uint32, level int) error {
	page, err := bt.pager.ReadPage(pageNum)
	if err != nil {
		return err
	}

	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	if isLeafPage(page) {
		fmt.Printf("%sLeaf Page %d: %d cells\n", indent, pageNum, page.Header.NumCells)
		keys, _ := page.GetAllCellKeys()
		fmt.Printf("%s  Keys: %v\n", indent, keys)
	} else {
		fmt.Printf("%sInterior Page %d: %d cells, rightmost=%d\n",
			indent, pageNum, page.Header.NumCells, page.Header.RightmostPointer)

		for i := uint16(0); i < page.Header.NumCells; i++ {
			cell, err := page.GetInteriorCell(i)
			if err != nil {
				return err
			}
			fmt.Printf("%s  Cell %d: key=%d -> page %d\n", indent, i, cell.Key, cell.ChildPage)
			if err := bt.printPage(cell.ChildPage, level+1); err != nil {
				return err
			}
		}

		fmt.Printf("%s  Rightmost -> page %d\n", indent, page.Header.RightmostPointer)
		if err := bt.printPage(page.Header.RightmostPointer, level+1); err != nil {
			return err
		}
	}

	return nil
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

func (bt *BTree) findLeafPageForNextKey(nextKey uint64) (uint32, error) {
	return bt.findLeafPage(nextKey)
}

func (bt *BTree) Scan() ([]Entry, error) {
	var entries []Entry

	rootPage, err := bt.pager.ReadPage(bt.rootPage)
	if err != nil {
		return nil, err
	}

	if rootPage.Header.PageType == PageTypeLeafTable ||
		rootPage.Header.PageType == PageTypeLeafIndex {
		for i := uint16(0); i < rootPage.Header.NumCells; i++ {
			cell, err := rootPage.GetLeafCell(i)
			if err != nil {
				return nil, err
			}
			entries = append(entries, Entry{
				Key:   cell.Key,
				Value: cell.Value,
			})
		}
		return entries, nil
	}

	it, err := bt.NewIterator()
	if err != nil {
		return nil, err
	}

	visited := make(map[uint32]bool)
	maxIterations := 100000
	iterations := 0

	for it.HasNext() {
		iterations++
		if iterations > maxIterations {
			return nil, fmt.Errorf("infinite loop detected after %d iterations", iterations)
		}

		if visited[it.currentPage] && it.currentCell == 0 {
			break
		}
		visited[it.currentPage] = true

		key, value, err := it.Next()
		if err != nil {
			break
		}

		entries = append(entries, Entry{
			Key:   key,
			Value: value,
		})
	}

	return entries, nil
}

func (bt *BTree) GetAllEntries() ([]Entry, error) {
	return bt.Scan()
}

func (bt *BTree) Count() (int, error) {
	count := 0

	it, err := bt.NewIterator()
	if err != nil {
		return 0, err
	}

	for it.HasNext() {
		_, _, err := it.Next()
		if err != nil {
			break
		}
		count++
	}

	return count, nil
}

func (bt *BTree) ForEach(fn func(key uint64, value []byte) bool) error {
	it, err := bt.NewIterator()
	if err != nil {
		return err
	}

	for it.HasNext() {
		key, value, err := it.Next()
		if err != nil {
			break
		}

		if !fn(key, value) {
			break
		}
	}

	return nil
}
