package engine

import (
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/catalog"
	"github.com/kithinjibrian/anubisdb/internal/parser"
)

type PlanNode interface {
	Type() string
	Cost() float64
	String() string
}

type ScanType string

const (
	FullScan        ScanType = "FullScan"
	IndexScan       ScanType = "IndexScan"
	UniqueIndexScan ScanType = "UniqueIndexScan"
)

type ScanPlan struct {
	Table     string
	Alias     string
	ScanType  ScanType
	IndexName string
	Filter    *FilterPlan
	EstRows   int
	EstCost   float64
}

func (s *ScanPlan) Type() string  { return "Scan" }
func (s *ScanPlan) Cost() float64 { return s.EstCost }
func (s *ScanPlan) String() string {
	result := fmt.Sprintf("Scan(%s", s.Table)
	if s.Alias != "" {
		result += fmt.Sprintf(" AS %s", s.Alias)
	}
	result += fmt.Sprintf(", type=%s", s.ScanType)
	if s.IndexName != "" {
		result += fmt.Sprintf(", index=%s", s.IndexName)
	}
	if s.Filter != nil {
		result += fmt.Sprintf(", filter=%v", s.Filter.Conditions)
	}
	result += fmt.Sprintf(", rows=%d, cost=%.2f)", s.EstRows, s.EstCost)
	return result
}

type FilterPlan struct {
	Conditions  []Condition
	Selectivity float64
}

type ProjectPlan struct {
	Columns  []string
	Distinct bool
	Input    PlanNode
	EstCost  float64
}

func (p *ProjectPlan) Type() string  { return "Project" }
func (p *ProjectPlan) Cost() float64 { return p.EstCost }
func (p *ProjectPlan) String() string {
	distinct := ""
	if p.Distinct {
		distinct = "DISTINCT "
	}
	return fmt.Sprintf("Project(%s%v, cost=%.2f) <- %s", distinct, p.Columns, p.EstCost, p.Input.String())
}

type JoinPlan struct {
	JoinType  string
	Left      PlanNode
	Right     *ScanPlan
	Condition Condition
	EstRows   int
	EstCost   float64
}

func (j *JoinPlan) Type() string  { return "Join" }
func (j *JoinPlan) Cost() float64 { return j.EstCost }
func (j *JoinPlan) String() string {
	return fmt.Sprintf("Join(%s, on=%s, rows=%d, cost=%.2f)\n  Left: %s\n  Right: %s",
		j.JoinType, j.Condition, j.EstRows, j.EstCost, j.Left.String(), j.Right.String())
}

type SortPlan struct {
	OrderBy []OrderItem
	Input   PlanNode
	EstCost float64
}

type OrderItem struct {
	Column    string
	Direction string
}

func (s *SortPlan) Type() string  { return "Sort" }
func (s *SortPlan) Cost() float64 { return s.EstCost }
func (s *SortPlan) String() string {
	return fmt.Sprintf("Sort(%v, cost=%.2f) <- %s", s.OrderBy, s.EstCost, s.Input.String())
}

type LimitPlan struct {
	Count   string
	Offset  string
	Input   PlanNode
	EstCost float64
}

func (l *LimitPlan) Type() string  { return "Limit" }
func (l *LimitPlan) Cost() float64 { return l.EstCost }
func (l *LimitPlan) String() string {
	result := fmt.Sprintf("Limit(%s", l.Count)
	if l.Offset != "" {
		result += fmt.Sprintf(", offset=%s", l.Offset)
	}
	result += fmt.Sprintf(", cost=%.2f) <- %s", l.EstCost, l.Input.String())
	return result
}

type GroupByPlan struct {
	Columns []string
	Input   PlanNode
	Having  *FilterPlan
	EstRows int
	EstCost float64
}

func (g *GroupByPlan) Type() string  { return "GroupBy" }
func (g *GroupByPlan) Cost() float64 { return g.EstCost }
func (g *GroupByPlan) String() string {
	result := fmt.Sprintf("GroupBy(%v, rows=%d, cost=%.2f)", g.Columns, g.EstRows, g.EstCost)
	if g.Having != nil {
		result += fmt.Sprintf(" HAVING %v", g.Having.Conditions)
	}
	result += fmt.Sprintf(" <- %s", g.Input.String())
	return result
}

type InsertPlan struct {
	Table   string
	Columns []string
	Values  []string
	EstCost float64
}

func (i *InsertPlan) Type() string  { return "Insert" }
func (i *InsertPlan) Cost() float64 { return i.EstCost }
func (i *InsertPlan) String() string {
	return fmt.Sprintf("Insert(%s, columns=%v, values=%v, cost=%.2f)",
		i.Table, i.Columns, i.Values, i.EstCost)
}

type DeletePlan struct {
	Scan    *ScanPlan
	EstCost float64
}

func (d *DeletePlan) Type() string  { return "Delete" }
func (d *DeletePlan) Cost() float64 { return d.EstCost }
func (d *DeletePlan) String() string {
	return fmt.Sprintf("Delete(cost=%.2f) <- %s", d.EstCost, d.Scan.String())
}

type UpdatePlan struct {
	Table       string
	Assignments []Assignment
	Scan        *ScanPlan
	EstCost     float64
}

func (u *UpdatePlan) Type() string  { return "Update" }
func (u *UpdatePlan) Cost() float64 { return u.EstCost }
func (u *UpdatePlan) String() string {
	return fmt.Sprintf("Update(%s, assignments=%v, cost=%.2f) <- %s",
		u.Table, u.Assignments, u.EstCost, u.Scan.String())
}

type CreateTablePlan struct {
	Table   string
	Columns []parser.ColumnDef
	EstCost float64
}

func (c *CreateTablePlan) Type() string  { return "CreateTable" }
func (c *CreateTablePlan) Cost() float64 { return c.EstCost }
func (c *CreateTablePlan) String() string {
	return fmt.Sprintf("CreateTable(%s, columns=%d, cost=%.2f)",
		c.Table, len(c.Columns), c.EstCost)
}

type CreateIndexPlan struct {
	IndexName string
	TableName string
	Columns   []string
	Unique    bool
	EstCost   float64
}

func (c *CreateIndexPlan) Type() string  { return "CreateIndex" }
func (c *CreateIndexPlan) Cost() float64 { return c.EstCost }
func (c *CreateIndexPlan) String() string {
	unique := ""
	if c.Unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CreateIndex(%s%s ON %s(%v), cost=%.2f)",
		unique, c.IndexName, c.TableName, c.Columns, c.EstCost)
}

type Condition struct {
	Column   string
	Operator string
	Value    string
}

func (c Condition) String() string {
	return fmt.Sprintf("%s %s %s", c.Column, c.Operator, c.Value)
}

type Assignment struct {
	Column string
	Value  string
}

type TableStats struct {
	Name     string
	RowCount int
	Indexes  map[string]*IndexInfo
}

type IndexInfo struct {
	Name        string
	Columns     []string
	Unique      bool
	Selectivity float64
}

type Planner struct {
	stats map[string]*TableStats
}

func NewPlanner(catalog *catalog.Catalog) *Planner {
	return &Planner{
		stats: make(map[string]*TableStats),
	}
}

func (p *Planner) RegisterTable(name string, rowCount int) {
	p.stats[name] = &TableStats{
		Name:     name,
		RowCount: rowCount,
		Indexes:  make(map[string]*IndexInfo),
	}
}

func (p *Planner) RegisterIndex(table, indexName string, columns []string, unique bool) {
	if stats, ok := p.stats[table]; ok {
		selectivity := 0.1
		if unique {
			selectivity = 1.0 / float64(stats.RowCount)
		}
		stats.Indexes[indexName] = &IndexInfo{
			Name:        indexName,
			Columns:     columns,
			Unique:      unique,
			Selectivity: selectivity,
		}
	}
}

func (p *Planner) Plan(node parser.Node) (PlanNode, error) {
	switch stmt := node.(type) {
	case *parser.SelectStmt:
		return p.planSelect(stmt)
	case *parser.InsertStmt:
		return p.planInsert(stmt)
	case *parser.DeleteStmt:
		return p.planDelete(stmt)
	case *parser.CreateTableStmt:
		return p.planCreateTable(stmt)
	case *parser.CreateIndexStmt:
		return p.planCreateIndex(stmt)
	case *parser.UpdateStmt:
		return p.planUpdate(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type for planning")
	}
}

func (p *Planner) planSelect(stmt *parser.SelectStmt) (PlanNode, error) {

	scan, err := p.planScanWithAlias(stmt.Table, stmt.Where)
	if err != nil {
		return nil, err
	}

	var currentPlan PlanNode = scan

	if len(stmt.Joins) > 0 {
		for _, join := range stmt.Joins {
			joinPlan, err := p.planJoin(currentPlan, join)
			if err != nil {
				return nil, err
			}
			currentPlan = joinPlan
		}
	}

	if len(stmt.GroupBy) > 0 {
		groupPlan, err := p.planGroupBy(stmt.GroupBy, stmt.Having, currentPlan)
		if err != nil {
			return nil, err
		}
		currentPlan = groupPlan
	}

	if len(stmt.OrderBy) > 0 {
		sortPlan := p.planSort(stmt.OrderBy, currentPlan)
		currentPlan = sortPlan
	}

	projectCost := currentPlan.Cost() + p.estimateRows(currentPlan)*0.01
	if stmt.Distinct {

		projectCost += p.estimateRows(currentPlan) * 0.5
	}
	project := &ProjectPlan{
		Columns:  stmt.Columns,
		Distinct: stmt.Distinct,
		Input:    currentPlan,
		EstCost:  projectCost,
	}
	currentPlan = project

	if stmt.Limit != nil {
		limitPlan := &LimitPlan{
			Count:   stmt.Limit.Count,
			Offset:  stmt.Limit.Offset,
			Input:   currentPlan,
			EstCost: currentPlan.Cost() * 0.1,
		}
		currentPlan = limitPlan
	}

	return currentPlan, nil
}

func (p *Planner) planScanWithAlias(tableRef *parser.TableRef, where *parser.WhereClause) (*ScanPlan, error) {
	stats, ok := p.stats[tableRef.Name]
	if !ok {

		stats = &TableStats{
			Name:     tableRef.Name,
			RowCount: 1000,
			Indexes:  make(map[string]*IndexInfo),
		}
	}

	scan := &ScanPlan{
		Table:   tableRef.Name,
		Alias:   tableRef.Alias,
		EstRows: stats.RowCount,
	}

	if where == nil || len(where.Conditions) == 0 {
		scan.ScanType = FullScan
		scan.EstCost = float64(stats.RowCount) * 1.0
		return scan, nil
	}

	conditions := make([]Condition, len(where.Conditions))
	for i, c := range where.Conditions {
		conditions[i] = Condition{
			Column:   c.Column,
			Operator: c.Operator,
			Value:    c.Value,
		}
	}

	bestIndex := p.findBestIndex(stats, conditions)

	if bestIndex != nil {
		scan.ScanType = IndexScan
		scan.IndexName = bestIndex.Name
		if bestIndex.Unique {
			scan.ScanType = UniqueIndexScan
		}
		scan.EstRows = int(float64(stats.RowCount) * bestIndex.Selectivity)
		scan.EstCost = float64(scan.EstRows) * 0.1
	} else {
		scan.ScanType = FullScan
		selectivity := p.estimateSelectivity(conditions)
		scan.EstRows = int(float64(stats.RowCount) * selectivity)
		scan.EstCost = float64(stats.RowCount) * 1.0
	}

	scan.Filter = &FilterPlan{
		Conditions:  conditions,
		Selectivity: float64(scan.EstRows) / float64(stats.RowCount),
	}

	return scan, nil
}

func (p *Planner) planScan(table string, where *parser.WhereClause) (*ScanPlan, error) {
	tableRef := &parser.TableRef{Name: table}
	return p.planScanWithAlias(tableRef, where)
}

func (p *Planner) planJoin(left PlanNode, join *parser.JoinClause) (*JoinPlan, error) {

	rightScan, err := p.planScanWithAlias(join.Table, nil)
	if err != nil {
		return nil, err
	}

	leftRows := p.estimateRows(left)
	rightRows := float64(rightScan.EstRows)

	joinRows := int(leftRows * rightRows * 0.1)

	joinCost := left.Cost() + rightScan.Cost() + (leftRows * rightRows * 0.01)

	joinType := join.Type
	if joinType == "" {
		joinType = "INNER"
	}

	return &JoinPlan{
		JoinType: joinType,
		Left:     left,
		Right:    rightScan,
		Condition: Condition{
			Column:   join.Condition.Column,
			Operator: join.Condition.Operator,
			Value:    join.Condition.Value,
		},
		EstRows: joinRows,
		EstCost: joinCost,
	}, nil
}

func (p *Planner) planSort(orderBy []*parser.OrderItem, input PlanNode) *SortPlan {
	inputRows := p.estimateRows(input)

	sortCost := input.Cost() + float64(inputRows)*2.0
	if inputRows > 100 {

		sortCost *= 1.5
	}

	orderItems := make([]OrderItem, len(orderBy))
	for i, item := range orderBy {
		orderItems[i] = OrderItem{
			Column:    item.Column,
			Direction: item.Direction,
		}
	}

	return &SortPlan{
		OrderBy: orderItems,
		Input:   input,
		EstCost: sortCost,
	}
}

func (p *Planner) planGroupBy(groupBy []string, having *parser.WhereClause, input PlanNode) (*GroupByPlan, error) {
	inputRows := p.estimateRows(input)

	groupRows := int(inputRows / 10)
	if groupRows < 1 {
		groupRows = 1
	}

	groupCost := input.Cost() + inputRows*1.5

	plan := &GroupByPlan{
		Columns: groupBy,
		Input:   input,
		EstRows: groupRows,
		EstCost: groupCost,
	}

	if having != nil && len(having.Conditions) > 0 {
		conditions := make([]Condition, len(having.Conditions))
		for i, c := range having.Conditions {
			conditions[i] = Condition{
				Column:   c.Column,
				Operator: c.Operator,
				Value:    c.Value,
			}
		}

		selectivity := p.estimateSelectivity(conditions)
		plan.EstRows = int(float64(groupRows) * selectivity)
		plan.Having = &FilterPlan{
			Conditions:  conditions,
			Selectivity: selectivity,
		}
	}

	return plan, nil
}

func (p *Planner) estimateRows(plan PlanNode) float64 {
	switch n := plan.(type) {
	case *ScanPlan:
		return float64(n.EstRows)
	case *JoinPlan:
		return float64(n.EstRows)
	case *GroupByPlan:
		return float64(n.EstRows)
	case *ProjectPlan:
		return p.estimateRows(n.Input)
	case *SortPlan:
		return p.estimateRows(n.Input)
	case *LimitPlan:
		return p.estimateRows(n.Input) * 0.5
	default:
		return 100.0
	}
}

func (p *Planner) findBestIndex(stats *TableStats, conditions []Condition) *IndexInfo {
	var bestIndex *IndexInfo
	for _, cond := range conditions {
		for _, idx := range stats.Indexes {
			for _, col := range idx.Columns {
				if col == cond.Column {
					if bestIndex == nil || idx.Unique {
						bestIndex = idx
					}
				}
			}
		}
	}
	return bestIndex
}

func (p *Planner) estimateSelectivity(conditions []Condition) float64 {
	if len(conditions) == 0 {
		return 1.0
	}
	selectivity := 1.0
	for range conditions {
		selectivity *= 0.1
	}
	if selectivity < 0.001 {
		selectivity = 0.001
	}
	return selectivity
}

func (p *Planner) planInsert(stmt *parser.InsertStmt) (PlanNode, error) {
	stats, ok := p.stats[stmt.Table]
	baseCost := float64(len(stmt.Values)) * 1.0

	if ok {
		indexCost := float64(len(stats.Indexes)) * 0.5
		baseCost += indexCost
	}

	return &InsertPlan{
		Table:   stmt.Table,
		Columns: stmt.Columns,
		Values:  stmt.Values,
		EstCost: baseCost,
	}, nil
}

func (p *Planner) planDelete(stmt *parser.DeleteStmt) (PlanNode, error) {
	scan, err := p.planScan(stmt.Table, stmt.Where)
	if err != nil {
		return nil, err
	}

	deleteCost := scan.Cost() + float64(scan.EstRows)*2.0
	stats, ok := p.stats[stmt.Table]
	if ok {
		deleteCost += float64(scan.EstRows) * float64(len(stats.Indexes)) * 0.5
	}

	return &DeletePlan{
		Scan:    scan,
		EstCost: deleteCost,
	}, nil
}

func (p *Planner) planUpdate(stmt *parser.UpdateStmt) (PlanNode, error) {
	scan, err := p.planScan(stmt.Table, stmt.Where)
	if err != nil {
		return nil, err
	}

	engineAssignments := make([]Assignment, len(stmt.Assignments))
	for i, a := range stmt.Assignments {
		engineAssignments[i] = Assignment{
			Column: a.Column,
			Value:  a.Value,
		}
	}

	updateCost := scan.Cost() + float64(scan.EstRows)*3.0
	return &UpdatePlan{
		Table:       stmt.Table,
		Assignments: engineAssignments,
		Scan:        scan,
		EstCost:     updateCost,
	}, nil
}

func (p *Planner) planCreateTable(stmt *parser.CreateTableStmt) (PlanNode, error) {
	baseCost := 10.0
	columnCost := float64(len(stmt.Columns)) * 1.0

	constraintCost := 0.0
	for _, col := range stmt.Columns {
		if col.PrimaryKey {
			constraintCost += 5.0
		}
		if col.Unique {
			constraintCost += 3.0
		}
	}

	return &CreateTablePlan{
		Table:   stmt.Table,
		Columns: stmt.Columns,
		EstCost: baseCost + columnCost + constraintCost,
	}, nil
}

func (p *Planner) planCreateIndex(stmt *parser.CreateIndexStmt) (PlanNode, error) {
	stats, ok := p.stats[stmt.TableName]

	baseCost := 10.0
	if ok {

		baseCost += float64(stats.RowCount) * 0.5
	}

	return &CreateIndexPlan{
		IndexName: stmt.IndexName,
		TableName: stmt.TableName,
		Columns:   stmt.Columns,
		Unique:    stmt.Unique,
		EstCost:   baseCost,
	}, nil
}

func Explain(plan PlanNode) string {
	return fmt.Sprintf("Execution Plan:\n%s\nTotal Cost: %.2f",
		plan.String(), plan.Cost())
}
