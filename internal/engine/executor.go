package engine

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kithinjibrian/anubisdb/internal/catalog"
	"github.com/kithinjibrian/anubisdb/internal/storage"
)

func ExecutePlan(e *Engine, plan PlanNode) (string, error) {
	switch p := plan.(type) {
	case *ProjectPlan:
		return executeProject(e, p)
	case *InsertPlan:
		return executeInsert(e, p)
	case *DeletePlan:
		return executeDelete(e, p)
	case *UpdatePlan:
		return executeUpdate(e, p)
	case *ScanPlan:
		return executeScan(e, p)
	case *CreateTablePlan:
		return executeCreateTable(e, p)
	default:
		return "", fmt.Errorf("unsupported plan type: %s", plan.Type())
	}
}

func executeProject(e *Engine, plan *ProjectPlan) (string, error) {

	rows, schema, err := fetchRawData(e, plan.Input)
	if err != nil {
		return "", err
	}

	if len(plan.Columns) == 1 && plan.Columns[0] == "*" {
		return formatResults(rows, schema), nil
	}

	var projectedRows []map[string]interface{}
	for _, row := range rows {
		newRow := make(map[string]interface{})
		for _, col := range plan.Columns {
			if val, ok := row[col]; ok {
				newRow[col] = val
			}
		}
		projectedRows = append(projectedRows, newRow)
	}

	projectedSchema := &catalog.Schema{
		Name:    schema.Name,
		Columns: []catalog.Column{},
	}
	for _, colName := range plan.Columns {
		for _, colDef := range schema.Columns {
			if colDef.Name == colName {
				projectedSchema.Columns = append(projectedSchema.Columns, colDef)
			}
		}
	}

	return formatResults(projectedRows, projectedSchema), nil
}

func executeScan(e *Engine, plan *ScanPlan) (string, error) {
	rows, schema, err := fetchRawData(e, plan)
	if err != nil {
		return "", err
	}
	return formatResults(rows, schema), nil
}

func fetchRawData(e *Engine, plan PlanNode) ([]map[string]interface{}, *catalog.Schema, error) {
	var scanPlan *ScanPlan
	if s, ok := plan.(*ScanPlan); ok {
		scanPlan = s
	} else if p, ok := plan.(*ProjectPlan); ok {
		return fetchRawData(e, p.Input)
	} else {
		return nil, nil, fmt.Errorf("unsupported source for raw data")
	}

	schema, err := e.catalog.GetSchema(scanPlan.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("table not found: %w", err)
	}

	btree, err := storage.LoadBTree(e.storage.Pager, schema.RootPage)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load B-Tree: %w", err)
	}

	entries, err := btree.Scan()
	if err != nil {
		return nil, nil, fmt.Errorf("scan failed: %w", err)
	}

	var rows []map[string]interface{}
	for _, entry := range entries {
		row, err := deserializeRow(entry.Value)
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}

	if scanPlan.Filter != nil {
		rows = applyFilter(rows, scanPlan.Filter)
	}

	return rows, schema, nil
}

func executeInsert(e *Engine, plan *InsertPlan) (string, error) {
	schema, err := e.catalog.GetTable(plan.Table)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	if len(plan.Values) != len(schema.Columns) {
		return "", fmt.Errorf("column count mismatch: expected %d, got %d",
			len(schema.Columns), len(plan.Values))
	}

	btree, err := storage.LoadBTree(e.storage.Pager, schema.RootPage)
	if err != nil {
		return "", fmt.Errorf("failed to load B-Tree: %w", err)
	}

	key, err := parseKey(plan.Values[0])
	if err != nil {
		return "", fmt.Errorf("invalid key value: %w", err)
	}

	value, err := serializeRow(plan.Values, schema)
	if err != nil {
		return "", fmt.Errorf("failed to serialize row: %w", err)
	}

	if err := btree.Insert(key, value); err != nil {
		return "", fmt.Errorf("insert failed: %w", err)
	}

	if btree.GetRootPage() != schema.RootPage {
		schema.RootPage = btree.GetRootPage()
		// e.catalog.UpdateTableRoot(plan.Table, schema.RootPage)
	}

	return "1 row inserted", nil
}

func executeUpdate(e *Engine, plan *UpdatePlan) (string, error) {
	schema, err := e.catalog.GetSchema(plan.Table)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	btree, err := storage.LoadBTree(e.storage.Pager, schema.RootPage)
	if err != nil {
		return "", fmt.Errorf("failed to load B-Tree: %w", err)
	}

	entries, err := btree.Scan()
	if err != nil {
		return "", fmt.Errorf("scan failed: %w", err)
	}

	updatedCount := 0
	for _, entry := range entries {
		row, err := deserializeRow(entry.Value)
		if err != nil {
			continue
		}

		if plan.Scan.Filter == nil || matchesFilter(row, plan.Scan.Filter) {
			for _, asgn := range plan.Assignments {
				row[asgn.Column] = asgn.Value
			}

			updatedValue, err := json.Marshal(row)
			if err != nil {
				return "", fmt.Errorf("failed to serialize updated row: %w", err)
			}

			if err := btree.Update(entry.Key, updatedValue); err != nil {
				return "", fmt.Errorf("failed to update key %d: %w", entry.Key, err)
			}
			updatedCount++
		}
	}

	return fmt.Sprintf("%d row(s) updated", updatedCount), nil
}

func executeDelete(e *Engine, plan *DeletePlan) (string, error) {
	schema, err := e.catalog.GetSchema(plan.Scan.Table)
	if err != nil {
		return "", fmt.Errorf("table not found: %w", err)
	}

	btree, err := storage.LoadBTree(e.storage.Pager, schema.RootPage)
	if err != nil {
		return "", fmt.Errorf("failed to load B-Tree: %w", err)
	}

	entries, err := btree.Scan()
	if err != nil {
		return "", fmt.Errorf("scan failed: %w", err)
	}

	var keysToDelete []uint64
	for _, entry := range entries {
		row, _ := deserializeRow(entry.Value)
		if plan.Scan.Filter == nil || matchesFilter(row, plan.Scan.Filter) {
			keysToDelete = append(keysToDelete, entry.Key)
		}
	}

	for _, key := range keysToDelete {
		if err := btree.Delete(key); err != nil {
			return "", fmt.Errorf("failed to delete key %d: %w", key, err)
		}
	}

	return fmt.Sprintf("%d row(s) deleted", len(keysToDelete)), nil
}

func executeCreateTable(e *Engine, plan *CreateTablePlan) (string, error) {
	columns := make([]catalog.Column, len(plan.Columns))
	for i, col := range plan.Columns {
		columns[i] = catalog.Column{
			Name:       col.Name,
			Type:       convertType(col.Type),
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

func convertType(t string) catalog.ColumnType {
	switch t {
	case "INT", "INTEGER":
		return catalog.TypeInt
	case "TEXT", "VARCHAR", "STRING":
		return catalog.TypeText
	default:
		return catalog.TypeText
	}
}

func parseKey(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

func serializeRow(values []string, schema *catalog.Schema) ([]byte, error) {
	rowData := make(map[string]interface{})
	for i, col := range schema.Columns {
		if i >= len(values) {
			break
		}
		var val interface{}
		switch col.Type {
		case catalog.TypeInt:
			val, _ = strconv.ParseInt(values[i], 10, 64)
		default:
			val = values[i]
		}
		rowData[col.Name] = val
	}
	return json.Marshal(rowData)
}

func deserializeRow(data []byte) (map[string]interface{}, error) {
	var rowData map[string]interface{}
	err := json.Unmarshal(data, &rowData)
	return rowData, err
}

func applyFilter(rows []map[string]interface{}, filter *FilterPlan) []map[string]interface{} {
	var filtered []map[string]interface{}
	for _, row := range rows {
		if matchesFilter(row, filter) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func matchesFilter(row map[string]interface{}, filter *FilterPlan) bool {
	for _, cond := range filter.Conditions {
		rowValue := row[cond.Column]
		if rowValue == nil {
			return false
		}
		rowStr := fmt.Sprintf("%v", rowValue)
		condStr := cond.Value

		switch cond.Operator {
		case "=":
			if rowStr != condStr {
				return false
			}
		case "!=", "<>":
			if rowStr == condStr {
				return false
			}
		case ">":
			if rowStr <= condStr {
				return false
			}
		case ">=":
			if rowStr < condStr {
				return false
			}
		case "<":
			if rowStr >= condStr {
				return false
			}
		case "<=":
			if rowStr > condStr {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func formatResults(rows []map[string]interface{}, schema *catalog.Schema) string {
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
			result += fmt.Sprintf("%-15v", row[col.Name])
		}
		result += "\n"
	}
	result += fmt.Sprintf("\n%d row(s) returned", len(rows))
	return result
}
