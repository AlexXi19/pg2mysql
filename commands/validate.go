package commands

import (
	"fmt"
	"pg2mysql"
	"sort"
	"strconv"
	"strings"
)

type ValidateCommand struct{}

func (c *ValidateCommand) Execute([]string) error {
	mysql := pg2mysql.NewMySQLDB(
		PG2MySQL.Config.MySQL.Database,
		PG2MySQL.Config.MySQL.Username,
		PG2MySQL.Config.MySQL.Password,
		PG2MySQL.Config.MySQL.Host,
		PG2MySQL.Config.MySQL.Port,
		PG2MySQL.Config.MySQL.Params,
	)

	err := mysql.Open()
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %s", err)
	}
	defer mysql.Close()

	pg := pg2mysql.NewPostgreSQLDB(
		PG2MySQL.Config.PostgreSQL.Database,
		PG2MySQL.Config.PostgreSQL.Username,
		PG2MySQL.Config.PostgreSQL.Password,
		PG2MySQL.Config.PostgreSQL.Host,
		PG2MySQL.Config.PostgreSQL.Port,
		PG2MySQL.Config.PostgreSQL.SSLMode,
	)
	err = pg.Open()
	if err != nil {
		return fmt.Errorf("failed to open pg connection: %s", err)
	}
	defer pg.Close()

	results, err := pg2mysql.NewValidator(pg, mysql).Validate(
		pg2mysql.MigrationConfig{
			IgnoreTables: PG2MySQL.Config.PostgreSQL.IgnoredTables,
		})
	if err != nil {
		return fmt.Errorf("failed to validate: %s", err)
	}

	for _, result := range sortValidatorResults(results) {
		switch {
		case len(result.IncompatibleRowIDs) > 0:
			truncatedIncompatibleRowIDs := truncateStringArray(result.IncompatibleRowIDs, 10)
            formattedColumnNames := formatIncompatibleColumnMetadata(result.IncompatibleColumnMetadata)
			fmt.Printf("found %d incompatible rows in %s with column names %v with IDs %v\n", result.IncompatibleRowCount, result.TableName, formattedColumnNames, truncatedIncompatibleRowIDs)

		case result.IncompatibleRowCount > 0:
			fmt.Printf("found %d incompatible rows in %s (which has no 'id' column)\n", result.IncompatibleRowCount, result.TableName)

		default:
			fmt.Printf("%s OK\n", result.TableName)
		}
	}

	return nil
}

func formatIncompatibleColumnMetadata(incompatibleColumnMetadata []pg2mysql.IncompatibleColumnMetadata) string {
    var str = "["
    for _, columnMetadata := range incompatibleColumnMetadata {
        str += "{ColumnName: " + columnMetadata.ColumnName + ", " + fmt.Sprintf("MaxChars: %v", columnMetadata.MaxChars) + "}"
    }

    return str + "]"
}

func sortValidatorResults(results []pg2mysql.ValidationResult) []pg2mysql.ValidationResult {
	sort.Slice(results, func(i, j int) bool {
		return results[i].TableName < results[j].TableName
	})

	return results
}

func truncateStringArray(arr []string, max int) string {
	if len(arr) > max {
		// if the array has more than the maximum elements, truncate it
		truncated := arr[:max]
		// calculate how many elements were truncated
		truncatedCount := len(arr) - max
		// create a string of the truncated count
		truncatedStr := strconv.Itoa(truncatedCount) + " more"
		// create a string of the truncated array
		truncatedArrStr := "[" + strings.Join(truncated, ", ") + ", ..."
		// append the truncated string to the end
		finalStr := truncatedArrStr + "]" + " and " + truncatedStr
		// print the final string
		return finalStr
	} else {
		// if the array has less than or equal to the maximum elements, just join and print it
		str := "[" + strings.Join(arr, ", ") + "]"
		return str
	}
}
