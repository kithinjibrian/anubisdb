package engine

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kithinjibrian/anubisdb/internal/catalog"
	"github.com/kithinjibrian/anubisdb/internal/storage"
)

func ExecutePlan(e *Engine, plan PlanNode) (string, error) {
	switch p := plan.(type) {
	case *CreateTablePlan:
		return executeCreateTable(e, p)
	case *CreateIndexPlan:
		return executeCreateIndex(e, p)
	case *InsertPlan:
		return executeInsert(e, p)
	case *ScanPlan:
		return executeScan(e, p)
	case *ProjectPlan:
		return executeProject(e, p)
	case *JoinPlan:
		return executeJoin(e, p)
	case *GroupByPlan:
		return executeGroupBy(e, p)
	case *SortPlan:
		return executeSort(e, p)
	case *LimitPlan:
		return executeLimit(e, p)
	case *UpdatePlan:
		return executeUpdate(e, p)
	case *DeletePlan:
		return executeDelete(e, p)
	default:
		return "", fmt.Errorf("unsupported plan type: %T", plan)
	}
}

// ResultSet represents query results with schema
type ResultSet struct {
	Schema  []string
	Rows    []map[string]interface{}
	Aliases map[string]string // table alias -> table name
}

func executeCreateTable(e *Engine, plan *CreateTablePlan) (string, error) {
	columns := make([]catalog.Column, len(plan.Columns))
	for i, col := range plan.Columns {
		columns[i] = catalog.Column{
			Name:       col.Name,
			Type:       parseColumnType(col.Type),
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
			Unique:     col.Unique,
		}
	}

	_, err := e.catalog.CreateTable(plan.Table, columns)
	if err != nil {
		return "", fmt.Errorf("failed to create table: %w", err)
	}

	return fmt.Sprintf("Table '%s' created successfully", plan.Table), nil
}

func executeCreateIndex(e *Engine, plan *CreateIndexPlan) (string, error) {
	table, err := e.catalog.LoadTable(plan.TableName)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	schema := table.GetSchema()

	for _, colName := range plan.Columns {
		if schema.GetColumn(colName) == nil {
			return "", fmt.Errorf("column '%s' not found in table '%s'", colName, plan.TableName)
		}
	}

	if len(plan.Columns) > 0 {
		if _, err := e.catalog.CreateIndex(plan.IndexName, plan.TableName, plan.Columns[0], plan.Unique); err != nil {
			return "", fmt.Errorf("failed to create index: %w", err)
		}
	}

	indexType := "INDEX"
	if plan.Unique {
		indexType = "UNIQUE INDEX"
	}
	return fmt.Sprintf("%s '%s' created successfully on %s(%v)", indexType, plan.IndexName, plan.TableName, plan.Columns), nil
}

func executeInsert(e *Engine, plan *InsertPlan) (string, error) {
	table, err := e.catalog.LoadTable(plan.Table)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	schema := table.GetSchema()

	if len(plan.Values) != schema.ColumnCount() {
		return "", fmt.Errorf("column count mismatch: expected %d, got %d",
			schema.ColumnCount(), len(plan.Values))
	}

	values, err := convertValues(plan.Values, schema)
	if err != nil {
		return "", fmt.Errorf("failed to convert values: %w", err)
	}

	if err := table.Insert(values); err != nil {
		return "", fmt.Errorf("insert failed: %w", err)
	}

	return "1 row inserted", nil
}

func executeScan(e *Engine, plan *ScanPlan) (string, error) {
	table, err := e.catalog.LoadTable(plan.Table)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	rows, err := executeFilteredScan(table, plan.Filter)
	if err != nil {
		return "", fmt.Errorf("scan failed: %w", err)
	}

	return formatTableResults(rows, table.GetSchema()), nil
}

func executeProject(e *Engine, plan *ProjectPlan) (string, error) {
	// Execute the input plan
	resultSet, err := executePlanToResultSet(e, plan.Input)
	if err != nil {
		return "", err
	}

	// Handle SELECT *
	if len(plan.Columns) == 1 && plan.Columns[0] == "*" {
		if plan.Distinct {
			resultSet.Rows = distinctRows(resultSet.Rows)
		}
		return formatResultSet(resultSet), nil
	}

	// Project specific columns
	projectedRows := make([]map[string]interface{}, 0, len(resultSet.Rows))
	for _, row := range resultSet.Rows {
		projectedRow := make(map[string]interface{})
		for _, col := range plan.Columns {
			if val, exists := row[col]; exists {
				projectedRow[col] = val
			} else {
				return "", fmt.Errorf("column '%s' not found", col)
			}
		}
		projectedRows = append(projectedRows, projectedRow)
	}

	if plan.Distinct {
		projectedRows = distinctRows(projectedRows)
	}

	resultSet.Schema = plan.Columns
	resultSet.Rows = projectedRows
	return formatResultSet(resultSet), nil
}

func executeJoin(e *Engine, plan *JoinPlan) (string, error) {
	// Execute left side
	leftResult, err := executePlanToResultSet(e, plan.Left)
	if err != nil {
		return "", err
	}

	// Execute right side (scan)
	rightTable, err := e.catalog.LoadTable(plan.Right.Table)
	if err != nil {
		return "", fmt.Errorf("right table not found: %w", err)
	}

	rightRows, err := executeFilteredScan(rightTable, plan.Right.Filter)
	if err != nil {
		return "", fmt.Errorf("right scan failed: %w", err)
	}

	// Convert right rows to map format
	rightResult := catalogRowsToResultSet(rightRows, rightTable.GetSchema(), plan.Right.Table, plan.Right.Alias)

	// Perform join
	joinedRows := make([]map[string]interface{}, 0)

	for _, leftRow := range leftResult.Rows {
		matched := false
		for _, rightRow := range rightResult.Rows {
			if evaluateJoinCondition(leftRow, rightRow, plan.Condition) {
				matched = true
				// Merge rows
				joinedRow := make(map[string]interface{})
				for k, v := range leftRow {
					joinedRow[k] = v
				}
				for k, v := range rightRow {
					joinedRow[k] = v
				}
				joinedRows = append(joinedRows, joinedRow)
			}
		}

		// For LEFT/RIGHT/FULL joins, handle unmatched rows
		if !matched && (plan.JoinType == "LEFT" || plan.JoinType == "FULL") {
			joinedRow := make(map[string]interface{})
			for k, v := range leftRow {
				joinedRow[k] = v
			}
			for _, col := range rightResult.Schema {
				joinedRow[col] = nil
			}
			joinedRows = append(joinedRows, joinedRow)
		}
	}

	// For RIGHT/FULL joins, add unmatched right rows
	if plan.JoinType == "RIGHT" || plan.JoinType == "FULL" {
		for _, rightRow := range rightResult.Rows {
			matched := false
			for _, leftRow := range leftResult.Rows {
				if evaluateJoinCondition(leftRow, rightRow, plan.Condition) {
					matched = true
					break
				}
			}
			if !matched {
				joinedRow := make(map[string]interface{})
				for _, col := range leftResult.Schema {
					joinedRow[col] = nil
				}
				for k, v := range rightRow {
					joinedRow[k] = v
				}
				joinedRows = append(joinedRows, joinedRow)
			}
		}
	}

	// Combine schemas
	combinedSchema := append(leftResult.Schema, rightResult.Schema...)

	resultSet := &ResultSet{
		Schema: combinedSchema,
		Rows:   joinedRows,
	}

	return formatResultSet(resultSet), nil
}

func executeGroupBy(e *Engine, plan *GroupByPlan) (string, error) {
	// Execute input
	resultSet, err := executePlanToResultSet(e, plan.Input)
	if err != nil {
		return "", err
	}

	// Group rows by specified columns
	groups := make(map[string][]map[string]interface{})

	for _, row := range resultSet.Rows {
		// Create group key
		keyParts := make([]string, len(plan.Columns))
		for i, col := range plan.Columns {
			keyParts[i] = fmt.Sprintf("%v", row[col])
		}
		groupKey := strings.Join(keyParts, "|")

		groups[groupKey] = append(groups[groupKey], row)
	}

	// Create result rows (one per group)
	groupedRows := make([]map[string]interface{}, 0, len(groups))
	for _, groupRows := range groups {
		if len(groupRows) > 0 {
			// For now, just take the first row of each group
			// In a real implementation, we'd compute aggregates here
			groupRow := make(map[string]interface{})
			for _, col := range plan.Columns {
				groupRow[col] = groupRows[0][col]
			}
			groupRow["COUNT(*)"] = len(groupRows)
			groupedRows = append(groupedRows, groupRow)
		}
	}

	// Apply HAVING filter if present
	if plan.Having != nil {
		filteredRows := make([]map[string]interface{}, 0)
		for _, row := range groupedRows {
			if matchesFilterMap(row, plan.Having) {
				filteredRows = append(filteredRows, row)
			}
		}
		groupedRows = filteredRows
	}

	resultSet.Rows = groupedRows
	resultSet.Schema = append(plan.Columns, "COUNT(*)")

	return formatResultSet(resultSet), nil
}

func executeSort(e *Engine, plan *SortPlan) (string, error) {
	// Execute input
	resultSet, err := executePlanToResultSet(e, plan.Input)
	if err != nil {
		return "", err
	}

	// Sort rows
	sort.Slice(resultSet.Rows, func(i, j int) bool {
		for _, orderItem := range plan.OrderBy {
			vi := resultSet.Rows[i][orderItem.Column]
			vj := resultSet.Rows[j][orderItem.Column]

			cmp := compareValues(vi, vj)

			if cmp != 0 {
				if orderItem.Direction == "DESC" {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return formatResultSet(resultSet), nil
}

func executeLimit(e *Engine, plan *LimitPlan) (string, error) {
	// Execute input
	resultSet, err := executePlanToResultSet(e, plan.Input)
	if err != nil {
		return "", err
	}

	limit, err := strconv.Atoi(plan.Count)
	if err != nil {
		return "", fmt.Errorf("invalid LIMIT value: %w", err)
	}

	offset := 0
	if plan.Offset != "" {
		offset, err = strconv.Atoi(plan.Offset)
		if err != nil {
			return "", fmt.Errorf("invalid OFFSET value: %w", err)
		}
	}

	// Apply offset and limit
	start := offset
	if start > len(resultSet.Rows) {
		start = len(resultSet.Rows)
	}

	end := start + limit
	if end > len(resultSet.Rows) {
		end = len(resultSet.Rows)
	}

	resultSet.Rows = resultSet.Rows[start:end]

	return formatResultSet(resultSet), nil
}

// Helper function to execute a plan and return ResultSet
func executePlanToResultSet(e *Engine, plan PlanNode) (*ResultSet, error) {
	switch p := plan.(type) {
	case *ScanPlan:
		table, err := e.catalog.LoadTable(p.Table)
		if err != nil {
			return nil, err
		}
		rows, err := executeFilteredScan(table, p.Filter)
		if err != nil {
			return nil, err
		}
		return catalogRowsToResultSet(rows, table.GetSchema(), p.Table, p.Alias), nil

	case *JoinPlan:
		// Execute join recursively
		leftResult, err := executePlanToResultSet(e, p.Left)
		if err != nil {
			return nil, err
		}

		rightTable, err := e.catalog.LoadTable(p.Right.Table)
		if err != nil {
			return nil, err
		}
		rightRows, err := executeFilteredScan(rightTable, p.Right.Filter)
		if err != nil {
			return nil, err
		}
		rightResult := catalogRowsToResultSet(rightRows, rightTable.GetSchema(), p.Right.Table, p.Right.Alias)

		joinedRows := make([]map[string]interface{}, 0)
		for _, leftRow := range leftResult.Rows {
			for _, rightRow := range rightResult.Rows {
				if evaluateJoinCondition(leftRow, rightRow, p.Condition) {
					joinedRow := make(map[string]interface{})
					for k, v := range leftRow {
						joinedRow[k] = v
					}
					for k, v := range rightRow {
						joinedRow[k] = v
					}
					joinedRows = append(joinedRows, joinedRow)
				}
			}
		}

		return &ResultSet{
			Schema: append(leftResult.Schema, rightResult.Schema...),
			Rows:   joinedRows,
		}, nil

	case *GroupByPlan:
		inputResult, err := executePlanToResultSet(e, p.Input)
		if err != nil {
			return nil, err
		}

		groups := make(map[string][]map[string]interface{})
		for _, row := range inputResult.Rows {
			keyParts := make([]string, len(p.Columns))
			for i, col := range p.Columns {
				keyParts[i] = fmt.Sprintf("%v", row[col])
			}
			groupKey := strings.Join(keyParts, "|")
			groups[groupKey] = append(groups[groupKey], row)
		}

		groupedRows := make([]map[string]interface{}, 0, len(groups))
		for _, groupRows := range groups {
			if len(groupRows) > 0 {
				groupRow := make(map[string]interface{})
				for _, col := range p.Columns {
					groupRow[col] = groupRows[0][col]
				}
				groupRow["COUNT(*)"] = len(groupRows)
				groupedRows = append(groupedRows, groupRow)
			}
		}

		if p.Having != nil {
			filteredRows := make([]map[string]interface{}, 0)
			for _, row := range groupedRows {
				if matchesFilterMap(row, p.Having) {
					filteredRows = append(filteredRows, row)
				}
			}
			groupedRows = filteredRows
		}

		return &ResultSet{
			Schema: append(p.Columns, "COUNT(*)"),
			Rows:   groupedRows,
		}, nil

	case *SortPlan:
		inputResult, err := executePlanToResultSet(e, p.Input)
		if err != nil {
			return nil, err
		}

		sort.Slice(inputResult.Rows, func(i, j int) bool {
			for _, orderItem := range p.OrderBy {
				vi := inputResult.Rows[i][orderItem.Column]
				vj := inputResult.Rows[j][orderItem.Column]
				cmp := compareValues(vi, vj)
				if cmp != 0 {
					if orderItem.Direction == "DESC" {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})

		return inputResult, nil

	case *LimitPlan:
		inputResult, err := executePlanToResultSet(e, p.Input)
		if err != nil {
			return nil, err
		}

		limit, _ := strconv.Atoi(p.Count)
		offset := 0
		if p.Offset != "" {
			offset, _ = strconv.Atoi(p.Offset)
		}

		start := offset
		if start > len(inputResult.Rows) {
			start = len(inputResult.Rows)
		}
		end := start + limit
		if end > len(inputResult.Rows) {
			end = len(inputResult.Rows)
		}

		inputResult.Rows = inputResult.Rows[start:end]
		return inputResult, nil

	case *ProjectPlan:
		inputResult, err := executePlanToResultSet(e, p.Input)
		if err != nil {
			return nil, err
		}

		if len(p.Columns) == 1 && p.Columns[0] == "*" {
			if p.Distinct {
				inputResult.Rows = distinctRows(inputResult.Rows)
			}
			return inputResult, nil
		}

		projectedRows := make([]map[string]interface{}, 0, len(inputResult.Rows))
		for _, row := range inputResult.Rows {
			projectedRow := make(map[string]interface{})
			for _, col := range p.Columns {
				projectedRow[col] = row[col]
			}
			projectedRows = append(projectedRows, projectedRow)
		}

		if p.Distinct {
			projectedRows = distinctRows(projectedRows)
		}

		return &ResultSet{
			Schema: p.Columns,
			Rows:   projectedRows,
		}, nil

	default:
		return nil, fmt.Errorf("cannot convert plan type %T to ResultSet", plan)
	}
}

func catalogRowsToResultSet(rows []*catalog.Row, schema *catalog.Schema, tableName, alias string) *ResultSet {
	prefix := tableName
	if alias != "" {
		prefix = alias
	}

	resultRows := make([]map[string]interface{}, len(rows))
	schemaNames := make([]string, len(schema.Columns))

	for i, col := range schema.Columns {
		schemaNames[i] = prefix + "." + col.Name
	}

	for i, row := range rows {
		resultRow := make(map[string]interface{})
		for _, col := range schema.Columns {
			key := prefix + "." + col.Name
			if rv, exists := row.Values[col.Name]; exists {
				resultRow[key] = rv.Value
			} else {
				resultRow[key] = nil
			}
			// Also add without prefix for convenience
			resultRow[col.Name] = resultRow[key]
		}
		resultRows[i] = resultRow
	}

	return &ResultSet{
		Schema: schemaNames,
		Rows:   resultRows,
	}
}

func evaluateJoinCondition(leftRow, rightRow map[string]interface{}, cond Condition) bool {
	leftVal := leftRow[cond.Column]
	rightVal := rightRow[cond.Value]

	if leftVal == nil || rightVal == nil {
		return false
	}

	return compareValues(leftVal, rightVal) == 0
}

func matchesFilterMap(row map[string]interface{}, filter *FilterPlan) bool {
	for _, cond := range filter.Conditions {
		val, exists := row[cond.Column]
		if !exists {
			return false
		}

		// Convert condition value to appropriate type
		condVal := cond.Value
		if !evaluateConditionMap(val, cond.Operator, condVal) {
			return false
		}
	}
	return true
}

func evaluateConditionMap(rowValue interface{}, operator, condValue string) bool {
	if rowValue == nil {
		return false
	}

	switch v := rowValue.(type) {
	case int64:
		condInt, _ := strconv.ParseInt(condValue, 10, 64)
		return compareInt(v, operator, condInt)
	case float64:
		condFloat, _ := strconv.ParseFloat(condValue, 64)
		return compareFloat(v, operator, condFloat)
	case string:
		return compareString(v, operator, condValue)
	case bool:
		condBool, _ := parseBool(condValue)
		return compareBool(v, operator, condBool)
	default:
		return false
	}
}

func distinctRows(rows []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]bool)
	result := make([]map[string]interface{}, 0)

	for _, row := range rows {
		// Create a key from all values
		keyParts := make([]string, 0, len(row))
		for _, v := range row {
			keyParts = append(keyParts, fmt.Sprintf("%v", v))
		}
		key := strings.Join(keyParts, "|")

		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case float64:
		if bv, ok := b.(float64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			if av == bv {
				return 0
			}
			if !av && bv {
				return -1
			}
			return 1
		}
	}

	return 0
}

func formatResultSet(rs *ResultSet) string {
	if len(rs.Rows) == 0 {
		return "No rows found"
	}

	result := ""

	// Header
	for i, col := range rs.Schema {
		if i > 0 {
			result += " | "
		}
		result += fmt.Sprintf("%-15s", col)
	}
	result += "\n"

	// Separator
	for range rs.Schema {
		result += "----------------"
	}
	result += "\n"

	// Rows
	for _, row := range rs.Rows {
		for i, col := range rs.Schema {
			if i > 0 {
				result += " | "
			}

			value := "NULL"
			if v, exists := row[col]; exists && v != nil {
				value = fmt.Sprintf("%v", v)
			}

			result += fmt.Sprintf("%-15s", value)
		}
		result += "\n"
	}

	result += fmt.Sprintf("\n%d row(s) returned", len(rs.Rows))
	return result
}

func executeFilteredScan(table *catalog.Table, filter *FilterPlan) ([]*catalog.Row, error) {
	schema := table.GetSchema()

	if filter == nil || len(filter.Conditions) == 0 {
		return table.Scan()
	}

	if len(filter.Conditions) == 1 {
		cond := filter.Conditions[0]

		if cond.Operator == "=" {
			pkCol := getPrimaryKeyColumn(schema)
			if pkCol != nil && cond.Column == pkCol.Name {
				key, err := createKeyFromValue(cond.Value, pkCol.Type)
				if err == nil {
					row, err := table.Get(key)
					if err != nil {
						return []*catalog.Row{}, nil
					}
					return []*catalog.Row{row}, nil
				}
			}
		}

		indexes := table.Catalog.GetTableIndexes(schema.Name)
		for _, idx := range indexes {
			if idx.ColumnName == cond.Column {
				col := schema.GetColumn(cond.Column)
				if col == nil {
					continue
				}

				switch cond.Operator {
				case "=":
					value, err := convertValue(cond.Value, col.Type)
					if err != nil {
						continue
					}
					row, err := table.GetByIndex(idx.Name, value)
					if err != nil {
						return []*catalog.Row{}, nil
					}
					return []*catalog.Row{row}, nil

				case ">", ">=", "<", "<=":
					rows, err := executeIndexRangeScan(table, idx, cond, col.Type)
					if err == nil {
						return rows, nil
					}
				}
			}
		}
	}

	rows, err := table.Scan()
	if err != nil {
		return nil, err
	}

	return filterRows(rows, filter), nil
}

func executeUpdate(e *Engine, plan *UpdatePlan) (string, error) {
	table, err := e.catalog.LoadTable(plan.Table)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	schema := table.GetSchema()

	rows, err := executeFilteredScan(table, plan.Scan.Filter)
	if err != nil {
		return "", fmt.Errorf("scan failed: %w", err)
	}

	updatedCount := 0
	var updateErrors []string

	for _, row := range rows {
		newRow := &catalog.Row{
			Values: make(map[string]catalog.RowValue),
		}

		for k, v := range row.Values {
			newRow.Values[k] = v
		}

		for _, assignment := range plan.Assignments {
			col := schema.GetColumn(assignment.Column)
			if col == nil {
				return "", fmt.Errorf("column '%s' not found", assignment.Column)
			}

			if col.PrimaryKey {
				return "", fmt.Errorf("cannot update primary key column '%s'", assignment.Column)
			}

			typedValue, err := convertValue(assignment.Value, col.Type)
			if err != nil {
				return "", fmt.Errorf("invalid value for column '%s': %w", assignment.Column, err)
			}

			newRow.Values[assignment.Column] = catalog.RowValue{
				Type:  col.Type,
				Value: typedValue,
			}
		}

		primaryKey, err := catalog.GetPrimaryKeyValue(row, schema)
		if err != nil {
			return "", fmt.Errorf("failed to get primary key: %w", err)
		}

		newValues := make([]interface{}, schema.ColumnCount())
		for i, col := range schema.Columns {
			rv, exists := newRow.Values[col.Name]
			if !exists {
				if col.NotNull {
					return "", fmt.Errorf("missing value for NOT NULL column '%s'", col.Name)
				}
				newValues[i] = nil
			} else {
				newValues[i] = rv.Value
			}
		}

		if err := table.Update(primaryKey, newValues); err != nil {
			updateErrors = append(updateErrors, fmt.Sprintf("row %v: %v", primaryKey, err))
			continue
		}

		updatedCount++
	}

	if len(updateErrors) > 0 {
		errMsg := fmt.Sprintf("%d row(s) updated, %d error(s): %s",
			updatedCount, len(updateErrors), strings.Join(updateErrors, "; "))
		return errMsg, nil
	}

	return fmt.Sprintf("%d row(s) updated", updatedCount), nil
}

func executeDelete(e *Engine, plan *DeletePlan) (string, error) {
	table, err := e.catalog.LoadTable(plan.Scan.Table)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	schema := table.GetSchema()

	rows, err := executeFilteredScan(table, plan.Scan.Filter)
	if err != nil {
		return "", fmt.Errorf("scan failed: %w", err)
	}

	var keysToDelete []storage.Key
	for _, row := range rows {
		primaryKey, err := catalog.GetPrimaryKeyValue(row, schema)
		if err != nil {
			return "", fmt.Errorf("failed to get primary key: %w", err)
		}
		keysToDelete = append(keysToDelete, primaryKey)
	}

	deletedCount := 0
	var deleteErrors []string

	for _, key := range keysToDelete {
		if err := table.Delete(key); err != nil {
			deleteErrors = append(deleteErrors, fmt.Sprintf("key %v: %v", key, err))
			continue
		}
		deletedCount++
	}

	if len(deleteErrors) > 0 {
		errMsg := fmt.Sprintf("%d row(s) deleted, %d error(s): %s",
			deletedCount, len(deleteErrors), strings.Join(deleteErrors, "; "))
		return errMsg, nil
	}

	return fmt.Sprintf("%d row(s) deleted", deletedCount), nil
}

// Utility functions from original executor

func executeIndexRangeScan(table *catalog.Table, idx *catalog.IndexMetadata,
	cond Condition, colType catalog.ColumnType) ([]*catalog.Row, error) {

	col := table.GetSchema().GetColumn(idx.ColumnName)
	if col == nil {
		return nil, fmt.Errorf("column not found")
	}

	value, err := convertValue(cond.Value, colType)
	if err != nil {
		return nil, err
	}

	var startValue, endValue interface{}

	switch cond.Operator {
	case ">":
		startValue = getNextValue(value, colType)
		endValue = getMaxValue(colType)
	case ">=":
		startValue = value
		endValue = getMaxValue(colType)
	case "<":
		startValue = getMinValue(colType)
		endValue = getPrevValue(value, colType)
	case "<=":
		startValue = getMinValue(colType)
		endValue = value
	default:
		return nil, fmt.Errorf("unsupported range operator: %s", cond.Operator)
	}

	rows, err := table.RangeByIndex(idx.Name, startValue, endValue)
	if err != nil {
		return nil, err
	}

	filtered := make([]*catalog.Row, 0, len(rows))
	for _, row := range rows {
		if matchesCondition(row, cond) {
			filtered = append(filtered, row)
		}
	}

	return filtered, nil
}

func getPrimaryKeyColumn(schema *catalog.Schema) *catalog.Column {
	for i := range schema.Columns {
		if schema.Columns[i].PrimaryKey {
			return &schema.Columns[i]
		}
	}
	return nil
}

func createKeyFromValue(value string, colType catalog.ColumnType) (storage.Key, error) {
	typedValue, err := convertValue(value, colType)
	if err != nil {
		return nil, err
	}
	return catalog.ValueToKey(typedValue, colType)
}

func getMinValue(colType catalog.ColumnType) interface{} {
	switch colType {
	case catalog.TypeInt:
		return int64(-9223372036854775808)
	case catalog.TypeFloat:
		return float64(-1.7976931348623157e+308)
	case catalog.TypeText:
		return ""
	case catalog.TypeBoolean:
		return false
	default:
		return nil
	}
}

func getMaxValue(colType catalog.ColumnType) interface{} {
	switch colType {
	case catalog.TypeInt:
		return int64(9223372036854775807)
	case catalog.TypeFloat:
		return float64(1.7976931348623157e+308)
	case catalog.TypeText:
		return string([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	case catalog.TypeBoolean:
		return true
	default:
		return nil
	}
}

func getNextValue(value interface{}, colType catalog.ColumnType) interface{} {
	switch colType {
	case catalog.TypeInt:
		if v, ok := value.(int64); ok {
			return v + 1
		}
	case catalog.TypeFloat:
		if v, ok := value.(float64); ok {
			return v + 0.0000000001
		}
	case catalog.TypeText:
		if v, ok := value.(string); ok {
			return v + string([]byte{0x00})
		}
	}
	return value
}

func getPrevValue(value interface{}, colType catalog.ColumnType) interface{} {
	switch colType {
	case catalog.TypeInt:
		if v, ok := value.(int64); ok {
			return v - 1
		}
	case catalog.TypeFloat:
		if v, ok := value.(float64); ok {
			return v - 0.0000000001
		}
	case catalog.TypeText:
		if v, ok := value.(string); ok && len(v) > 0 {
			return v[:len(v)-1]
		}
	}
	return value
}

func matchesCondition(row *catalog.Row, cond Condition) bool {
	rowValue, exists := row.Values[cond.Column]
	if !exists {
		return false
	}
	return evaluateCondition(rowValue.Value, cond.Operator, cond.Value, rowValue.Type)
}

func parseColumnType(typeStr string) catalog.ColumnType {
	switch strings.ToUpper(typeStr) {
	case "INT", "INTEGER":
		return catalog.TypeInt
	case "TEXT", "VARCHAR", "STRING", "CHAR":
		return catalog.TypeText
	case "FLOAT", "REAL", "DOUBLE":
		return catalog.TypeFloat
	case "BOOLEAN", "BOOL":
		return catalog.TypeBoolean
	default:
		return catalog.TypeText
	}
}

func convertValues(values []string, schema *catalog.Schema) ([]interface{}, error) {
	result := make([]interface{}, len(values))

	for i, col := range schema.Columns {
		if i >= len(values) {
			return nil, fmt.Errorf("missing value for column '%s'", col.Name)
		}

		if strings.ToUpper(values[i]) == "NULL" {
			if col.NotNull {
				return nil, fmt.Errorf("NULL value not allowed for NOT NULL column '%s'", col.Name)
			}
			result[i] = nil
			continue
		}

		typedValue, err := convertValue(values[i], col.Type)
		if err != nil {
			return nil, fmt.Errorf("invalid value for column '%s': %w", col.Name, err)
		}

		result[i] = typedValue
	}

	return result, nil
}

func convertValue(value string, colType catalog.ColumnType) (interface{}, error) {
	if strings.ToUpper(value) == "NULL" {
		return nil, nil
	}

	switch colType {
	case catalog.TypeInt:
		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %w", err)
		}
		return intVal, nil

	case catalog.TypeFloat:
		floatVal, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %w", err)
		}
		return floatVal, nil

	case catalog.TypeBoolean:
		switch strings.ToUpper(value) {
		case "TRUE", "1", "T", "YES", "Y":
			return true, nil
		case "FALSE", "0", "F", "NO", "N":
			return false, nil
		default:
			return nil, fmt.Errorf("invalid boolean: %s", value)
		}

	case catalog.TypeText:
		return value, nil

	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func filterRows(rows []*catalog.Row, filter *FilterPlan) []*catalog.Row {
	if filter == nil {
		return rows
	}

	var filtered []*catalog.Row
	for _, row := range rows {
		if matchesFilter(row, filter) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func matchesFilter(row *catalog.Row, filter *FilterPlan) bool {
	for _, cond := range filter.Conditions {
		rowValue, exists := row.Values[cond.Column]
		if !exists {
			return false
		}

		if rowValue.Value == nil {
			return false
		}

		if !evaluateCondition(rowValue.Value, cond.Operator, cond.Value, rowValue.Type) {
			return false
		}
	}
	return true
}

func evaluateCondition(rowValue interface{}, operator, condValue string, colType catalog.ColumnType) bool {
	if rowValue == nil {
		return false
	}

	switch colType {
	case catalog.TypeInt:
		rowInt, ok := rowValue.(int64)
		if !ok {
			if f, ok := rowValue.(float64); ok {
				rowInt = int64(f)
			} else {
				return false
			}
		}

		condInt, err := strconv.ParseInt(condValue, 10, 64)
		if err != nil {
			return false
		}

		return compareInt(rowInt, operator, condInt)

	case catalog.TypeFloat:
		rowFloat, ok := rowValue.(float64)
		if !ok {
			return false
		}

		condFloat, err := strconv.ParseFloat(condValue, 64)
		if err != nil {
			return false
		}

		return compareFloat(rowFloat, operator, condFloat)

	case catalog.TypeText:
		rowStr, ok := rowValue.(string)
		if !ok {
			rowStr = fmt.Sprintf("%v", rowValue)
		}

		return compareString(rowStr, operator, condValue)

	case catalog.TypeBoolean:
		rowBool, ok := rowValue.(bool)
		if !ok {
			return false
		}

		condBool, err := parseBool(condValue)
		if err != nil {
			return false
		}

		return compareBool(rowBool, operator, condBool)

	default:
		return false
	}
}

func parseBool(value string) (bool, error) {
	switch strings.ToUpper(value) {
	case "TRUE", "1", "T", "YES", "Y":
		return true, nil
	case "FALSE", "0", "F", "NO", "N":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean: %s", value)
	}
}

func compareInt(a int64, op string, b int64) bool {
	switch op {
	case "=":
		return a == b
	case "!=", "<>":
		return a != b
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	default:
		return false
	}
}

const floatEpsilon = 0.0000001

func compareFloat(a float64, op string, b float64) bool {
	switch op {
	case "=":
		return abs(a-b) < floatEpsilon
	case "!=", "<>":
		return abs(a-b) >= floatEpsilon
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	default:
		return false
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func compareString(a, op, b string) bool {
	switch op {
	case "=":
		return a == b
	case "!=", "<>":
		return a != b
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	default:
		return false
	}
}

func compareBool(a bool, op string, b bool) bool {
	switch op {
	case "=":
		return a == b
	case "!=", "<>":
		return a != b
	default:
		return false
	}
}

func formatTableResults(rows []*catalog.Row, schema *catalog.Schema) string {
	if len(rows) == 0 {
		return "No rows found"
	}

	result := ""

	for i, col := range schema.Columns {
		if i > 0 {
			result += " | "
		}
		result += fmt.Sprintf("%-15s", col.Name)
	}
	result += "\n"

	for range schema.Columns {
		result += "----------------"
	}
	result += "\n"

	for _, row := range rows {
		for i, col := range schema.Columns {
			if i > 0 {
				result += " | "
			}

			value := "NULL"
			if rv, exists := row.Values[col.Name]; exists && rv.Value != nil {
				value = fmt.Sprintf("%v", rv.Value)
			}

			result += fmt.Sprintf("%-15s", value)
		}
		result += "\n"
	}

	result += fmt.Sprintf("\n%d row(s) returned", len(rows))
	return result
}
