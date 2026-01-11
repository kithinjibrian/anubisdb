package parser

import "fmt"

type Node interface {
	String() string
}

type SelectStmt struct {
	Columns []string
	Table   string
	Where   *WhereClause
}

func (s *SelectStmt) String() string {
	result := fmt.Sprintf("SELECT %v FROM %s", s.Columns, s.Table)
	if s.Where != nil {
		result += fmt.Sprintf(" WHERE %v", s.Where.Conditions)
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

func (p *Parser) parseCreate() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}
	p.nextToken()

	if !p.curKeywordIs("TABLE") {
		return nil, fmt.Errorf("expected TABLE, got %s", p.curTok.Literal)
	}
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
		cols = append(cols, p.curTok.Literal)
		p.nextToken()

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
		cond := Condition{}

		if p.curTok.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected column name, got %s", p.curTok.Literal)
		}
		cond.Column = p.curTok.Literal
		p.nextToken()

		if p.curTok.Type != OPERATOR {
			return nil, fmt.Errorf("expected operator, got %s", p.curTok.Literal)
		}
		cond.Operator = p.curTok.Literal
		p.nextToken()

		if p.curTok.Type == STRING || p.curTok.Type == NUMBER || p.curTok.Type == IDENTIFIER {
			cond.Value = p.curTok.Literal
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected value, got %s", p.curTok.Literal)
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
