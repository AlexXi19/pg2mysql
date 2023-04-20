package pg2mysql

import (
	"fmt"
)

type Validator interface {
	Validate(validationConfig ValidationConfig) ([]ValidationResult, error)
}

func NewValidator(src, dst DB) Validator {
	return &validator{
		src: src,
		dst: dst,
	}
}

type validator struct {
	src, dst DB
}

type ValidationConfig struct {
	IgnoreTables []string
}

func ignoreTable(table string, tables []string) bool {
	for _, t := range tables {
		if t == table {
			return true
		}
	}
	return false
}

func (v *validator) Validate(validationConfig ValidationConfig) ([]ValidationResult, error) {
	srcSchema, err := BuildSchema(v.src)
	if err != nil {
		return nil, fmt.Errorf("failed to build source schema: %s", err)
	}

	dstSchema, err := BuildSchema(v.dst)
	if err != nil {
		return nil, fmt.Errorf("failed to build destination schema: %s", err)
	}

	var results []ValidationResult
	for _, srcTable := range srcSchema.Tables {
		if ignoreTable(srcTable.Name, validationConfig.IgnoreTables) {
			continue
		}

		dstTable, err := dstSchema.GetTable(srcTable.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get table from destination schema: %s", err)
		}

		// TODO: Can refactor to using the primary key instead of using the id column
		hasSrcPrimaryKey, err := v.src.HasPrimaryKey(srcTable.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key from source table: %s", err)
		}

		if hasSrcPrimaryKey {
			rowIDs, columnNames, err := GetIncompatibleRowIDsAndColumns(v.src, srcTable, dstTable)
			if err != nil {
				return nil, fmt.Errorf("failed getting incompatible row ids: %s", err)
			}

			results = append(results, ValidationResult{
				TableName:            srcTable.Name,
				IncompatibleRowIDs:   rowIDs,
				IncompatibleRowCount: int64(len(rowIDs)),
				IncompatibleColumns:  columnNames,
			})
		} else {
			rowCount, columnNames, err := GetIncompatibleRowCount(v.src, srcTable, dstTable)
			if err != nil {
				return nil, fmt.Errorf("failed getting incompatible row count: %s", err)
			}

			results = append(results, ValidationResult{
				TableName:            srcTable.Name,
				IncompatibleRowCount: rowCount,
				IncompatibleColumns:  columnNames,
			})
		}
	}

	return results, nil
}

type ValidationResult struct {
	TableName            string
	IncompatibleRowIDs   []string
	IncompatibleColumns  []string
	IncompatibleRowCount int64
}
