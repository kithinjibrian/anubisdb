package parser

/*
statement     = select_stmt | insert_stmt | delete_stmt | create_table_stmt | update_stmt | create_index_stmt

select_stmt   = "SELECT" [ "DISTINCT" ] column_list "FROM" table_ref
                [ join_clause ]
                [ where_clause ]
                [ group_by_clause ]
                [ having_clause ]
                [ order_by_clause ]
                [ limit_clause ]

insert_stmt   = "INSERT" "INTO" identifier [ "(" column_list ")" ] "VALUES" "(" value_list ")"

delete_stmt   = "DELETE" "FROM" identifier [ where_clause ]

update_stmt   = "UPDATE" identifier "SET" assignment_list [ where_clause ]

create_table_stmt = "CREATE" "TABLE" identifier "(" column_def { "," column_def } ")"

create_index_stmt = "CREATE" [ "UNIQUE" ] "INDEX" identifier "ON" identifier "(" column_list ")"

table_ref     = identifier [ [ "AS" ] identifier ]

join_clause   = join_type "JOIN" table_ref "ON" condition

join_type     = [ "INNER" | "LEFT" | "RIGHT" | "FULL" ]

where_clause  = "WHERE" condition { ( "AND" | "OR" ) condition }

group_by_clause = "GROUP" "BY" column_list

having_clause = "HAVING" condition { ( "AND" | "OR" ) condition }

order_by_clause = "ORDER" "BY" order_item { "," order_item }

order_item    = identifier [ "ASC" | "DESC" ]

limit_clause  = "LIMIT" number [ "OFFSET" number ]

condition     = identifier operator value

assignment_list = assignment { "," assignment }

assignment    = identifier "=" value

column_list   = ( "*" | identifier { "," identifier } )

value_list    = value { "," value }

column_def    = identifier data_type { constraint }

constraint    = "PRIMARY" "KEY" | "UNIQUE" | "NOT" "NULL" | "AUTO_INCREMENT"

value         = string | number | identifier
operator      = "=" | "!=" | "<" | ">" | "<=" | ">=" | "LIKE" | "IN"
data_type     = "INT" | "VARCHAR" | "TEXT" | "BOOLEAN" | "DATE" | "DECIMAL" | "FLOAT"
identifier    = letter { letter | digit | "_" }
*/

import "fmt"

type Node interface {
	String() string
}

type SelectStmt struct {
	Distinct bool
	Columns  []string
	Table    *TableRef
	Joins    []*JoinClause
	Where    *WhereClause
	GroupBy  []string
	Having   *WhereClause
	OrderBy  []*OrderItem
	Limit    *LimitClause
}

func (s *SelectStmt) String() string {
	result := "SELECT "
	if s.Distinct {
		result += "DISTINCT "
	}
	result += fmt.Sprintf("%v FROM %s", s.Columns, s.Table)

	for _, join := range s.Joins {
		result += fmt.Sprintf(" %s", join)
	}

	if s.Where != nil {
		result += fmt.Sprintf(" WHERE %v", s.Where.Conditions)
	}

	if len(s.GroupBy) > 0 {
		result += fmt.Sprintf(" GROUP BY %v", s.GroupBy)
	}

	if s.Having != nil {
		result += fmt.Sprintf(" HAVING %v", s.Having.Conditions)
	}

	if len(s.OrderBy) > 0 {
		result += fmt.Sprintf(" ORDER BY %v", s.OrderBy)
	}

	if s.Limit != nil {
		result += fmt.Sprintf(" %s", s.Limit)
	}

	return result
}

type TableRef struct {
	Name  string
	Alias string
}

func (t *TableRef) String() string {
	if t.Alias != "" {
		return fmt.Sprintf("%s AS %s", t.Name, t.Alias)
	}
	return t.Name
}

type JoinClause struct {
	Type      string
	Table     *TableRef
	Condition Condition
}

func (j *JoinClause) String() string {
	joinType := j.Type
	if joinType == "" {
		joinType = "INNER"
	}
	return fmt.Sprintf("%s JOIN %s ON %s", joinType, j.Table, j.Condition)
}

type OrderItem struct {
	Column    string
	Direction string
}

func (o *OrderItem) String() string {
	if o.Direction != "" {
		return fmt.Sprintf("%s %s", o.Column, o.Direction)
	}
	return o.Column
}

type LimitClause struct {
	Count  string
	Offset string
}

func (l *LimitClause) String() string {
	result := fmt.Sprintf("LIMIT %s", l.Count)
	if l.Offset != "" {
		result += fmt.Sprintf(" OFFSET %s", l.Offset)
	}
	return result
}

type InsertStmt struct {
	Table   string
	Columns []string
	Values  []string
}

func (i *InsertStmt) String() string {
	return fmt.Sprintf("INSERT INTO %s (%v) VALUES (%v)", i.Table, i.Columns, i.Values)
}

type DeleteStmt struct {
	Table string
	Where *WhereClause
}

func (d *DeleteStmt) String() string {
	result := fmt.Sprintf("DELETE FROM %s", d.Table)
	if d.Where != nil {
		result += fmt.Sprintf(" WHERE %v", d.Where.Conditions)
	}
	return result
}

type CreateTableStmt struct {
	Table   string
	Columns []ColumnDef
}

func (c *CreateTableStmt) String() string {
	return fmt.Sprintf("CREATE TABLE %s (%v)", c.Table, c.Columns)
}

type CreateIndexStmt struct {
	IndexName string
	TableName string
	Columns   []string
	Unique    bool
}

func (c *CreateIndexStmt) String() string {
	unique := ""
	if c.Unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%v)", unique, c.IndexName, c.TableName, c.Columns)
}

type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       *WhereClause
}

type Assignment struct {
	Column string
	Value  string
}

func (u *UpdateStmt) String() string {
	result := fmt.Sprintf("UPDATE %s SET %v", u.Table, u.Assignments)
	if u.Where != nil {
		result += fmt.Sprintf(" WHERE %v", u.Where.Conditions)
	}
	return result
}

func (a Assignment) String() string {
	return fmt.Sprintf("%s = %s", a.Column, a.Value)
}

type ColumnDef struct {
	Name          string
	Type          string
	PrimaryKey    bool
	Unique        bool
	NotNull       bool
	AutoIncrement bool
}

func (c ColumnDef) String() string {
	result := fmt.Sprintf("%s %s", c.Name, c.Type)
	if c.PrimaryKey {
		result += " PRIMARY KEY"
	}
	if c.Unique {
		result += " UNIQUE"
	}
	if c.NotNull {
		result += " NOT NULL"
	}
	if c.AutoIncrement {
		result += " AUTO_INCREMENT"
	}
	return result
}

type WhereClause struct {
	Conditions []Condition
}

type Condition struct {
	Column   string
	Operator string
	Value    string
}

func (c Condition) String() string {
	return fmt.Sprintf("%s %s %s", c.Column, c.Operator, c.Value)
}

type Parser struct {
	lexer   *Lexer
	curTok  Token
	peekTok Token
}

func NewParser(input string) *Parser {
	p := &Parser{lexer: NewLexer(input)}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curTok = p.peekTok
	p.peekTok = p.lexer.NextToken()
}

func (p *Parser) curKeywordIs(keyword string) bool {
	return p.curTok.Type == KEYWORD && p.curTok.Value == keyword
}

func (p *Parser) peekKeywordIs(keyword string) bool {
	return p.peekTok.Type == KEYWORD && p.peekTok.Value == keyword
}

func (p *Parser) Parse() (Node, error) {
	switch {
	case p.curKeywordIs("SELECT"):
		return p.parseSelect()
	case p.curKeywordIs("INSERT"):
		return p.parseInsert()
	case p.curKeywordIs("DELETE"):
		return p.parseDelete()
	case p.curKeywordIs("CREATE"):
		return p.parseCreate()
	case p.curKeywordIs("UPDATE"):
		return p.parseUpdate()
	default:
		return nil, fmt.Errorf("unsupported statement: %s", p.curTok.Literal)
	}
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}
	p.nextToken()

	if p.curKeywordIs("DISTINCT") {
		stmt.Distinct = true
		p.nextToken()
	}

	if p.curTok.Type == ASTERISK {
		stmt.Columns = []string{"*"}
		p.nextToken()
	} else {
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
	}

	if !p.curKeywordIs("FROM") {
		return nil, fmt.Errorf("expected FROM, got %s", p.curTok.Literal)
	}
	p.nextToken()

	tableRef, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt.Table = tableRef

	for p.curKeywordIs("JOIN") || p.curKeywordIs("INNER") || p.curKeywordIs("LEFT") ||
		p.curKeywordIs("RIGHT") || p.curKeywordIs("FULL") {
		join, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		stmt.Joins = append(stmt.Joins, join)
	}

	if p.curKeywordIs("WHERE") {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	if p.curKeywordIs("GROUP") {
		p.nextToken()
		if !p.curKeywordIs("BY") {
			return nil, fmt.Errorf("expected BY after GROUP, got %s", p.curTok.Literal)
		}
		p.nextToken()

		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = cols
	}

	if p.curKeywordIs("HAVING") {
		having, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Having = having
	}

	if p.curKeywordIs("ORDER") {
		p.nextToken()
		if !p.curKeywordIs("BY") {
			return nil, fmt.Errorf("expected BY after ORDER, got %s", p.curTok.Literal)
		}
		p.nextToken()

		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	if p.curKeywordIs("LIMIT") {
		limit, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		stmt.Limit = limit
	}

	return stmt, nil
}

func (p *Parser) parseTableRef() (*TableRef, error) {
	if p.curTok.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}

	tableRef := &TableRef{Name: p.curTok.Literal}
	p.nextToken()

	if p.curKeywordIs("AS") {
		p.nextToken()
	}

	if p.curTok.Type == IDENTIFIER && !p.curKeywordIs("WHERE") && !p.curKeywordIs("JOIN") &&
		!p.curKeywordIs("INNER") && !p.curKeywordIs("LEFT") && !p.curKeywordIs("RIGHT") &&
		!p.curKeywordIs("ORDER") && !p.curKeywordIs("GROUP") && !p.curKeywordIs("LIMIT") &&
		!p.curKeywordIs("HAVING") {
		tableRef.Alias = p.curTok.Literal
		p.nextToken()
	}

	return tableRef, nil
}

func (p *Parser) parseJoin() (*JoinClause, error) {
	join := &JoinClause{}

	if p.curKeywordIs("INNER") || p.curKeywordIs("LEFT") || p.curKeywordIs("RIGHT") || p.curKeywordIs("FULL") {
		join.Type = p.curTok.Literal
		p.nextToken()
	}

	if !p.curKeywordIs("JOIN") {
		return nil, fmt.Errorf("expected JOIN, got %s", p.curTok.Literal)
	}
	p.nextToken()

	tableRef, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	join.Table = tableRef

	if !p.curKeywordIs("ON") {
		return nil, fmt.Errorf("expected ON, got %s", p.curTok.Literal)
	}
	p.nextToken()

	cond, err := p.parseCondition()
	if err != nil {
		return nil, err
	}
	join.Condition = cond

	return join, nil
}

func (p *Parser) parseCondition() (Condition, error) {
	cond := Condition{}

	if p.curTok.Type != IDENTIFIER {
		return cond, fmt.Errorf("expected column name, got %s", p.curTok.Literal)
	}

	colName := p.curTok.Literal
	p.nextToken()

	if p.curTok.Type == DOT {
		p.nextToken()
		if p.curTok.Type != IDENTIFIER {
			return cond, fmt.Errorf("expected column name after dot, got %s", p.curTok.Literal)
		}
		colName = colName + "." + p.curTok.Literal
		p.nextToken()
	}

	cond.Column = colName

	if p.curTok.Type != OPERATOR {
		return cond, fmt.Errorf("expected operator, got %s", p.curTok.Literal)
	}
	cond.Operator = p.curTok.Literal
	p.nextToken()

	if p.curTok.Type == STRING || p.curTok.Type == NUMBER || p.curTok.Type == IDENTIFIER {
		valueName := p.curTok.Literal
		p.nextToken()

		if p.curTok.Type == DOT {
			p.nextToken()
			if p.curTok.Type != IDENTIFIER {
				return cond, fmt.Errorf("expected identifier after dot, got %s", p.curTok.Literal)
			}
			valueName = valueName + "." + p.curTok.Literal
			p.nextToken()
		}

		cond.Value = valueName
	} else {
		return cond, fmt.Errorf("expected value, got %s", p.curTok.Literal)
	}

	return cond, nil
}

func (p *Parser) parseOrderBy() ([]*OrderItem, error) {
	items := []*OrderItem{}

	for {
		if p.curTok.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected column name, got %s", p.curTok.Literal)
		}

		item := &OrderItem{Column: p.curTok.Literal}
		p.nextToken()

		if p.curKeywordIs("ASC") || p.curKeywordIs("DESC") {
			item.Direction = p.curTok.Literal
			p.nextToken()
		}

		items = append(items, item)

		if p.curTok.Type != COMMA {
			break
		}
		p.nextToken()
	}

	return items, nil
}

func (p *Parser) parseLimit() (*LimitClause, error) {
	p.nextToken()

	if p.curTok.Type != NUMBER {
		return nil, fmt.Errorf("expected number after LIMIT, got %s", p.curTok.Literal)
	}

	limit := &LimitClause{Count: p.curTok.Literal}
	p.nextToken()

	if p.curKeywordIs("OFFSET") {
		p.nextToken()
		if p.curTok.Type != NUMBER {
			return nil, fmt.Errorf("expected number after OFFSET, got %s", p.curTok.Literal)
		}
		limit.Offset = p.curTok.Literal
		p.nextToken()
	}

	return limit, nil
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{}
	p.nextToken()

	if !p.curKeywordIs("INTO") {
		return nil, fmt.Errorf("expected INTO, got %s", p.curTok.Literal)
	}
	p.nextToken()

	if p.curTok.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}
	stmt.Table = p.curTok.Literal
	p.nextToken()

	if p.curTok.Type == LPAREN {
		p.nextToken()
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols

		if p.curTok.Type != RPAREN {
			return nil, fmt.Errorf("expected ), got %s", p.curTok.Literal)
		}
		p.nextToken()
	}

	if !p.curKeywordIs("VALUES") {
		return nil, fmt.Errorf("expected VALUES, got %s", p.curTok.Literal)
	}
	p.nextToken()

	if p.curTok.Type != LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.curTok.Literal)
	}
	p.nextToken()

	vals, err := p.parseValueList()
	if err != nil {
		return nil, err
	}
	stmt.Values = vals

	if p.curTok.Type != RPAREN {
		return nil, fmt.Errorf("expected ), got %s", p.curTok.Literal)
	}
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}
	p.nextToken()

	if !p.curKeywordIs("FROM") {
		return nil, fmt.Errorf("expected FROM, got %s", p.curTok.Literal)
	}
	p.nextToken()

	if p.curTok.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}
	stmt.Table = p.curTok.Literal
	p.nextToken()

	if p.curKeywordIs("WHERE") {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseCreate() (Node, error) {
	p.nextToken()

	if p.curKeywordIs("TABLE") {
		return p.parseCreateTable()
	} else if p.curKeywordIs("INDEX") || p.curKeywordIs("UNIQUE") {
		return p.parseCreateIndex()
	}

	return nil, fmt.Errorf("expected TABLE or INDEX after CREATE, got %s", p.curTok.Literal)
}

func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}
	p.nextToken()

	if p.curTok.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}
	stmt.Table = p.curTok.Literal
	p.nextToken()

	if p.curTok.Type != LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.curTok.Literal)
	}
	p.nextToken()

	cols, err := p.parseColumnDefList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	if p.curTok.Type != RPAREN {
		return nil, fmt.Errorf("expected ), got %s", p.curTok.Literal)
	}
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{}

	if p.curKeywordIs("UNIQUE") {
		stmt.Unique = true
		p.nextToken()
	}

	if !p.curKeywordIs("INDEX") {
		return nil, fmt.Errorf("expected INDEX, got %s", p.curTok.Literal)
	}
	p.nextToken()

	if p.curTok.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected index name, got %s", p.curTok.Literal)
	}
	stmt.IndexName = p.curTok.Literal
	p.nextToken()

	if !p.curKeywordIs("ON") {
		return nil, fmt.Errorf("expected ON, got %s", p.curTok.Literal)
	}
	p.nextToken()

	if p.curTok.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}
	stmt.TableName = p.curTok.Literal
	p.nextToken()

	if p.curTok.Type != LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.curTok.Literal)
	}
	p.nextToken()

	cols, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	if p.curTok.Type != RPAREN {
		return nil, fmt.Errorf("expected ), got %s", p.curTok.Literal)
	}
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseColumnDefList() ([]ColumnDef, error) {
	cols := []ColumnDef{}

	for {
		colDef := ColumnDef{}

		if p.curTok.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected column name, got %s", p.curTok.Literal)
		}
		colDef.Name = p.curTok.Literal
		p.nextToken()

		if p.curTok.Type != KEYWORD {
			return nil, fmt.Errorf("expected data type, got %s", p.curTok.Literal)
		}
		colDef.Type = p.curTok.Literal
		p.nextToken()

		for {
			if p.curKeywordIs("PRIMARY") && p.peekKeywordIs("KEY") {
				colDef.PrimaryKey = true
				p.nextToken()
				p.nextToken()
			} else if p.curKeywordIs("UNIQUE") {
				colDef.Unique = true
				p.nextToken()
			} else if p.curKeywordIs("NOT") && p.peekKeywordIs("NULL") {
				colDef.NotNull = true
				p.nextToken()
				p.nextToken()
			} else if p.curKeywordIs("AUTO_INCREMENT") {
				colDef.AutoIncrement = true
				p.nextToken()
			} else {
				break
			}
		}

		cols = append(cols, colDef)

		if p.curTok.Type != COMMA {
			break
		}
		p.nextToken()
	}

	return cols, nil
}

func (p *Parser) parseColumnList() ([]string, error) {
	cols := []string{}

	for {
		if p.curTok.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected identifier, got %s", p.curTok.Literal)
		}

		colName := p.curTok.Literal
		p.nextToken()

		if p.curTok.Type == DOT {
			p.nextToken()
			if p.curTok.Type != IDENTIFIER {
				return nil, fmt.Errorf("expected column name after dot, got %s", p.curTok.Literal)
			}
			colName = colName + "." + p.curTok.Literal
			p.nextToken()
		}

		cols = append(cols, colName)

		if p.curTok.Type != COMMA {
			break
		}
		p.nextToken()
	}

	return cols, nil
}

func (p *Parser) parseValueList() ([]string, error) {
	vals := []string{}

	for {
		if p.curTok.Type == STRING || p.curTok.Type == NUMBER || p.curTok.Type == IDENTIFIER {
			vals = append(vals, p.curTok.Literal)
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected value, got %s", p.curTok.Literal)
		}

		if p.curTok.Type != COMMA {
			break
		}
		p.nextToken()
	}

	return vals, nil
}

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{}
	p.nextToken()

	if p.curTok.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.curTok.Literal)
	}
	stmt.Table = p.curTok.Literal
	p.nextToken()

	if !p.curKeywordIs("SET") {
		return nil, fmt.Errorf("expected SET, got %s", p.curTok.Literal)
	}
	p.nextToken()

	for {
		asgn := Assignment{}
		if p.curTok.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected column name in SET, got %s", p.curTok.Literal)
		}
		asgn.Column = p.curTok.Literal
		p.nextToken()

		if p.curTok.Type != OPERATOR || p.curTok.Literal != "=" {
			return nil, fmt.Errorf("expected '=', got %s", p.curTok.Literal)
		}
		p.nextToken()

		if p.curTok.Type == STRING || p.curTok.Type == NUMBER || p.curTok.Type == IDENTIFIER {
			asgn.Value = p.curTok.Literal
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected value in SET, got %s", p.curTok.Literal)
		}

		stmt.Assignments = append(stmt.Assignments, asgn)

		if p.curTok.Type != COMMA {
			break
		}
		p.nextToken()
	}

	if p.curKeywordIs("WHERE") {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseWhere() (*WhereClause, error) {
	where := &WhereClause{}
	p.nextToken()

	for {
		cond, err := p.parseCondition()
		if err != nil {
			return nil, err
		}

		where.Conditions = append(where.Conditions, cond)

		if !p.curKeywordIs("AND") && !p.curKeywordIs("OR") {
			break
		}
		p.nextToken()
	}

	return where, nil
}

func Parse(input string) (Node, error) {
	parser := NewParser(input)
	return parser.Parse()
}
