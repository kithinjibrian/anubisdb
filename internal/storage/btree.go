package storage

import (
	"errors"
	"fmt"
)

type BTree struct {
	pager   *Pager
	root    uint32
	isIndex bool
}

type Entry struct {
	Key   Key
	Value []byte
}

func NewBTree(pager *Pager, isIndex bool) (*BTree, error) {
	pageType := PageTypeLeafTable
	if isIndex {
		pageType = PageTypeLeafIndex
	}

	rootNum, root, err := pager.AllocatePage(pageType, 0)
	if err != nil {
		return nil, err
	}

	root.Header.NextLeaf = 0
	root.Header.PrevLeaf = 0
	root.writeHeader()

	if err := pager.WritePage(rootNum, root); err != nil {
		return nil, err
	}

	return &BTree{
		pager:   pager,
		root:    rootNum,
		isIndex: isIndex,
	}, nil
}

func LoadBTree(pager *Pager, rootPage uint32, isIndex bool) (*BTree, error) {
	if rootPage == 0 {
		return nil, errors.New("invalid root page number")
	}

	root, err := pager.ReadPage(rootPage)
	if err != nil {
		return nil, fmt.Errorf("failed to load root page: %w", err)
	}

	expectedLeafType := PageTypeLeafTable
	expectedInteriorType := PageTypeInteriorTable
	if isIndex {
		expectedLeafType = PageTypeLeafIndex
		expectedInteriorType = PageTypeInteriorIndex
	}

	if root.Header.PageType != expectedLeafType && root.Header.PageType != expectedInteriorType {
		return nil, fmt.Errorf("root page has incorrect type: %d", root.Header.PageType)
	}

	return &BTree{
		pager:   pager,
		root:    rootPage,
		isIndex: isIndex,
	}, nil
}

func (tree *BTree) getLeafPageType() PageType {
	if tree.isIndex {
		return PageTypeLeafIndex
	}
	return PageTypeLeafTable
}

func (tree *BTree) getInteriorPageType() PageType {
	if tree.isIndex {
		return PageTypeInteriorIndex
	}
	return PageTypeInteriorTable
}

func (tree *BTree) Search(key Key) ([]byte, error) {
	leafNum, err := tree.navigateToLeaf(tree.root, key)
	if err != nil {
		return nil, err
	}

	leaf, err := tree.pager.ReadPage(leafNum)
	if err != nil {
		return nil, err
	}

	idx, found, err := leaf.SearchCell(key)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, errors.New("key not found")
	}

	cell, err := leaf.GetLeafCell(idx)
	if err != nil {
		return nil, err
	}

	return cell.Value, nil
}

func (tree *BTree) navigateToLeaf(nodeNum uint32, key Key) (uint32, error) {
	node, err := tree.pager.ReadPage(nodeNum)
	if err != nil {
		return 0, err
	}

	if isLeaf(node.Header.PageType) {
		return nodeNum, nil
	}

	childNum, err := tree.findChild(node, key)
	if err != nil {
		return 0, err
	}

	return tree.navigateToLeaf(childNum, key)
}

func (tree *BTree) findChild(node *Page, key Key) (uint32, error) {
	for i := uint16(0); i < node.Header.NumCells; i++ {
		cellKey, err := node.GetCellKey(i)
		if err != nil {
			return 0, fmt.Errorf("failed to get cell key: %w", err)
		}

		if key.Compare(cellKey) < 0 {
			cell, err := node.GetInteriorCell(i)
			if err != nil {
				return 0, fmt.Errorf("failed to get interior cell: %w", err)
			}
			return cell.ChildPage, nil
		}
	}

	return node.Header.RightmostPointer, nil
}

func (tree *BTree) Insert(key Key, value []byte) error {
	path := make([]*pathNode, 0)
	leafNum, err := tree.navigateWithPath(tree.root, key, &path)
	if err != nil {
		return err
	}

	leaf, err := tree.pager.ReadPage(leafNum)
	if err != nil {
		return err
	}

	if _, found, _ := leaf.SearchCell(key); found {
		return errors.New("duplicate key")
	}

	cell := NewLeafCell(key, value)

	if leaf.CanFit(cell.Size()) {
		if err := leaf.InsertLeafCell(cell); err != nil {
			return err
		}
		return tree.pager.WritePage(leafNum, leaf)
	}

	return tree.insertAndSplit(leafNum, leaf, cell, path)
}

type pathNode struct {
	pageNum uint32
	page    *Page
}

func (tree *BTree) navigateWithPath(nodeNum uint32, key Key, path *[]*pathNode) (uint32, error) {
	node, err := tree.pager.ReadPage(nodeNum)
	if err != nil {
		return 0, err
	}

	if isLeaf(node.Header.PageType) {
		return nodeNum, nil
	}

	*path = append(*path, &pathNode{pageNum: nodeNum, page: node})

	childNum, err := tree.findChild(node, key)
	if err != nil {
		return 0, err
	}

	return tree.navigateWithPath(childNum, key, path)
}

func (tree *BTree) insertAndSplit(leafNum uint32, leaf *Page, newCell *LeafCell, path []*pathNode) error {
	cells := make([]*LeafCell, 0, leaf.Header.NumCells+1)

	for i := uint16(0); i < leaf.Header.NumCells; i++ {
		c, err := leaf.GetLeafCell(i)
		if err != nil {
			return fmt.Errorf("failed to get leaf cell during split: %w", err)
		}
		cells = append(cells, c)
	}
	cells = append(cells, newCell)

	tree.sortLeafCells(cells)

	mid := len(cells) / 2

	if mid == 0 {
		mid = 1
	}
	if mid >= len(cells) {
		mid = len(cells) - 1
	}

	siblingNum, sibling, err := tree.pager.AllocatePage(tree.getLeafPageType(), 0)
	if err != nil {
		return err
	}

	tree.resetPage(leaf)
	tree.resetPage(sibling)

	for i := 0; i < mid; i++ {
		if err := leaf.InsertLeafCell(cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell into left leaf: %w", err)
		}
	}

	for i := mid; i < len(cells); i++ {
		if err := sibling.InsertLeafCell(cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell into right leaf: %w", err)
		}
	}

	sibling.Header.NextLeaf = leaf.Header.NextLeaf
	sibling.Header.PrevLeaf = leafNum
	leaf.Header.NextLeaf = siblingNum

	if sibling.Header.NextLeaf != 0 {
		next, err := tree.pager.ReadPage(sibling.Header.NextLeaf)
		if err != nil {
			return fmt.Errorf("failed to read next leaf during split: %w", err)
		}
		next.Header.PrevLeaf = siblingNum
		next.writeHeader()
		if err := tree.pager.WritePage(sibling.Header.NextLeaf, next); err != nil {
			return fmt.Errorf("failed to update next leaf pointer: %w", err)
		}
	}

	leaf.writeHeader()
	sibling.writeHeader()

	if err := tree.pager.WritePage(leafNum, leaf); err != nil {
		return err
	}
	if err := tree.pager.WritePage(siblingNum, sibling); err != nil {
		return err
	}

	splitKey := cells[mid].Key

	return tree.insertIntoParent(leafNum, splitKey, siblingNum, path)
}

func (tree *BTree) sortLeafCells(cells []*LeafCell) {

	for i := 1; i < len(cells); i++ {
		j := i
		for j > 0 && cells[j].Key.Compare(cells[j-1].Key) < 0 {
			cells[j], cells[j-1] = cells[j-1], cells[j]
			j--
		}
	}
}

func (tree *BTree) sortInternalCells(cells []*InteriorCell) {

	for i := 1; i < len(cells); i++ {
		j := i
		for j > 0 && cells[j].Key.Compare(cells[j-1].Key) < 0 {
			cells[j], cells[j-1] = cells[j-1], cells[j]
			j--
		}
	}
}

func (tree *BTree) resetPage(page *Page) {
	offset := page.GetHeaderSize()

	for i := offset; i < len(page.Data); i++ {
		page.Data[i] = 0
	}
	page.Header.NumCells = 0
	page.Header.CellContentOffset = uint16(PageSize)
	page.Header.FragmentedBytes = 0
	page.writeHeader()
}

func (tree *BTree) insertIntoParent(leftChild uint32, splitKey Key, rightChild uint32, path []*pathNode) error {
	if len(path) == 0 {
		return tree.createNewRoot(leftChild, splitKey, rightChild)
	}

	parent := path[len(path)-1]
	path = path[:len(path)-1]

	cell := NewInteriorCell(splitKey, leftChild)

	if parent.page.CanFit(cell.Size()) {
		if err := parent.page.InsertInteriorCell(cell); err != nil {
			return err
		}
		return tree.pager.WritePage(parent.pageNum, parent.page)
	}

	return tree.splitInternalNode(parent.pageNum, parent.page, cell, path)
}

func (tree *BTree) splitInternalNode(nodeNum uint32, node *Page, newCell *InteriorCell, path []*pathNode) error {
	cells := make([]*InteriorCell, 0, node.Header.NumCells+1)

	for i := uint16(0); i < node.Header.NumCells; i++ {
		c, err := node.GetInteriorCell(i)
		if err != nil {
			return fmt.Errorf("failed to get interior cell during split: %w", err)
		}
		cells = append(cells, c)
	}
	cells = append(cells, newCell)

	tree.sortInternalCells(cells)

	mid := len(cells) / 2

	if mid == 0 {
		mid = 1
	}
	if mid >= len(cells) {
		mid = len(cells) - 1
	}

	siblingNum, sibling, err := tree.pager.AllocatePage(tree.getInteriorPageType(), 0)
	if err != nil {
		return err
	}

	tree.resetPage(node)
	tree.resetPage(sibling)

	for i := 0; i < mid; i++ {
		if err := node.InsertInteriorCell(cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell into left interior: %w", err)
		}
	}

	pushUpKey := cells[mid].Key

	for i := mid + 1; i < len(cells); i++ {
		if err := sibling.InsertInteriorCell(cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell into right interior: %w", err)
		}
	}

	sibling.Header.RightmostPointer = node.Header.RightmostPointer
	node.Header.RightmostPointer = cells[mid].ChildPage

	node.writeHeader()
	sibling.writeHeader()

	if err := tree.pager.WritePage(nodeNum, node); err != nil {
		return err
	}
	if err := tree.pager.WritePage(siblingNum, sibling); err != nil {
		return err
	}

	return tree.insertIntoParent(nodeNum, pushUpKey, siblingNum, path)
}

func (tree *BTree) createNewRoot(leftChild uint32, key Key, rightChild uint32) error {
	newRootNum, newRoot, err := tree.pager.AllocatePage(tree.getInteriorPageType(), 0)
	if err != nil {
		return err
	}

	cell := NewInteriorCell(key, leftChild)
	if err := newRoot.InsertInteriorCell(cell); err != nil {
		return err
	}
	newRoot.Header.RightmostPointer = rightChild
	newRoot.writeHeader()

	if err := tree.pager.WritePage(newRootNum, newRoot); err != nil {
		return err
	}

	tree.root = newRootNum
	return nil
}

func (tree *BTree) Delete(key Key) error {
	leafNum, err := tree.navigateToLeaf(tree.root, key)
	if err != nil {
		return err
	}

	leaf, err := tree.pager.ReadPage(leafNum)
	if err != nil {
		return err
	}

	idx, found, err := leaf.SearchCell(key)
	if err != nil {
		return err
	}

	if !found {
		return errors.New("key not found")
	}

	if err := leaf.deleteCell(idx); err != nil {
		return err
	}

	if err := tree.pager.WritePage(leafNum, leaf); err != nil {
		return err
	}

	// TODO: Implement underflow handling (merge/redistribute)
	// For now, we allow nodes to become sparse

	return nil
}

func (tree *BTree) Update(key Key, newValue []byte) error {
	leafNum, err := tree.navigateToLeaf(tree.root, key)
	if err != nil {
		return err
	}

	leaf, err := tree.pager.ReadPage(leafNum)
	if err != nil {
		return err
	}

	idx, found, err := leaf.SearchCell(key)
	if err != nil {
		return err
	}

	if !found {
		return errors.New("key not found")
	}

	oldCell, err := leaf.GetLeafCell(idx)
	if err != nil {
		return err
	}

	newCell := NewLeafCell(key, newValue)

	if newCell.Size() <= oldCell.Size() || leaf.CanFit(newCell.Size()) {

		if err := leaf.deleteCell(idx); err != nil {
			return err
		}

		if err := leaf.InsertLeafCell(newCell); err != nil {
			return err
		}

		return tree.pager.WritePage(leafNum, leaf)
	}

	if err := leaf.deleteCell(idx); err != nil {
		return err
	}

	if err := leaf.InsertLeafCell(newCell); err != nil {

		leaf, err = tree.pager.ReadPage(leafNum)
		if err != nil {
			return err
		}

		path := make([]*pathNode, 0)
		leafNum, err = tree.navigateWithPath(tree.root, key, &path)
		if err != nil {
			return err
		}

		leaf, err = tree.pager.ReadPage(leafNum)
		if err != nil {
			return err
		}

		return tree.insertAndSplit(leafNum, leaf, newCell, path)
	}

	return tree.pager.WritePage(leafNum, leaf)
}

func (tree *BTree) Scan() ([]Entry, error) {
	leftmost, err := tree.findLeftmostLeaf()
	if err != nil {
		return nil, err
	}

	var result []Entry
	currentNum := leftmost

	visited := make(map[uint32]bool)

	for currentNum != 0 {
		if visited[currentNum] {
			return nil, fmt.Errorf("circular reference detected in leaf chain at page %d", currentNum)
		}
		visited[currentNum] = true

		current, err := tree.pager.ReadPage(currentNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d during scan: %w", currentNum, err)
		}

		for i := uint16(0); i < current.Header.NumCells; i++ {
			cell, err := current.GetLeafCell(i)
			if err != nil {
				return nil, fmt.Errorf("failed to read cell %d from page %d: %w", i, currentNum, err)
			}

			result = append(result, Entry{
				Key:   cell.Key,
				Value: cell.Value,
			})
		}

		currentNum = current.Header.NextLeaf
	}

	return result, nil
}

func (tree *BTree) findLeftmostLeaf() (uint32, error) {
	currentNum := tree.root

	for {
		current, err := tree.pager.ReadPage(currentNum)
		if err != nil {
			return 0, err
		}

		if isLeaf(current.Header.PageType) {
			return currentNum, nil
		}

		if current.Header.NumCells > 0 {
			cell, err := current.GetInteriorCell(0)
			if err != nil {
				return 0, fmt.Errorf("failed to get first interior cell: %w", err)
			}
			currentNum = cell.ChildPage
		} else {
			currentNum = current.Header.RightmostPointer
		}

		if currentNum == 0 {
			return 0, errors.New("invalid child pointer (0) encountered")
		}
	}
}

func (tree *BTree) RangeSearch(start, end Key) ([]Entry, error) {
	leafNum, err := tree.navigateToLeaf(tree.root, start)
	if err != nil {
		return nil, err
	}

	var result []Entry
	currentNum := leafNum
	visited := make(map[uint32]bool)

	for currentNum != 0 {
		if visited[currentNum] {
			return nil, fmt.Errorf("circular reference detected in leaf chain at page %d", currentNum)
		}
		visited[currentNum] = true

		current, err := tree.pager.ReadPage(currentNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d during range search: %w", currentNum, err)
		}

		for i := uint16(0); i < current.Header.NumCells; i++ {
			cell, err := current.GetLeafCell(i)
			if err != nil {
				return nil, fmt.Errorf("failed to read cell %d from page %d: %w", i, currentNum, err)
			}

			if cell.Key.Compare(start) < 0 {
				continue
			}

			if cell.Key.Compare(end) > 0 {
				return result, nil
			}

			result = append(result, Entry{
				Key:   cell.Key,
				Value: cell.Value,
			})
		}

		currentNum = current.Header.NextLeaf
	}

	return result, nil
}

func (tree *BTree) GetAllEntries() ([]Entry, error) {
	return tree.Scan()
}

func (tree *BTree) Count() (int, error) {
	entries, err := tree.Scan()
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func (tree *BTree) ForEach(fn func(key Key, value []byte) bool) error {
	entries, err := tree.Scan()
	if err != nil {
		return err
	}

	for _, e := range entries {
		if !fn(e.Key, e.Value) {
			break
		}
	}
	return nil
}

func (tree *BTree) GetRootPage() uint32 {
	return tree.root
}

func (tree *BTree) GetDepth() (uint32, error) {
	depth := uint32(0)
	currentNum := tree.root

	for {
		current, err := tree.pager.ReadPage(currentNum)
		if err != nil {
			return 0, err
		}

		depth++

		if isLeaf(current.Header.PageType) {
			return depth, nil
		}

		if current.Header.NumCells > 0 {
			cell, err := current.GetInteriorCell(0)
			if err != nil {
				return 0, fmt.Errorf("failed to get interior cell: %w", err)
			}
			currentNum = cell.ChildPage
		} else {
			currentNum = current.Header.RightmostPointer
		}

		if currentNum == 0 {
			return 0, errors.New("invalid child pointer encountered")
		}
	}
}

func (tree *BTree) PrintTree() error {
	depth, _ := tree.GetDepth()
	fmt.Printf("B+ Tree (root=%d, depth=%d)\n", tree.root, depth)
	return tree.printNode(tree.root, 0)
}

func (tree *BTree) printNode(nodeNum uint32, level int) error {
	node, err := tree.pager.ReadPage(nodeNum)
	if err != nil {
		return err
	}

	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	if isLeaf(node.Header.PageType) {
		fmt.Printf("%sLEAF[%d] cells=%d next=%d\n",
			indent, nodeNum, node.Header.NumCells, node.Header.NextLeaf)

		for i := uint16(0); i < node.Header.NumCells; i++ {
			cell, _ := node.GetLeafCell(i)
			fmt.Printf("%s  %s = %v\n", indent, cell.Key.String(), cell.Value)
		}
	} else {
		fmt.Printf("%sINTERNAL[%d] cells=%d\n", indent, nodeNum, node.Header.NumCells)

		for i := uint16(0); i < node.Header.NumCells; i++ {
			cell, _ := node.GetInteriorCell(i)
			fmt.Printf("%s  [%s] -> %d\n", indent, cell.Key.String(), cell.ChildPage)
			tree.printNode(cell.ChildPage, level+1)
		}

		fmt.Printf("%s  [*] -> %d\n", indent, node.Header.RightmostPointer)
		tree.printNode(node.Header.RightmostPointer, level+1)
	}

	return nil
}
