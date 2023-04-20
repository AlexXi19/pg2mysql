package pg2mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"sort"

	"github.com/go-sql-driver/mysql"
)

func NewMySQLDB(
	database string,
	username string,
	password string,
	host string,
	port int,
	params map[string]string,
) DB {
	if params == nil {
		params = make(map[string]string)
	}
	params["parseTime"] = "true"
	params["charset"] = "utf8"
	params["multiStatements"] = "true"

	config := mysql.Config{
		User:   username,
		Passwd: password,
		DBName: database,
		Net:    "tcp",
		Addr:   fmt.Sprintf("%s:%d", host, port),
		Params: params,
	}

	return &mySQLDB{
		dsn:    FormatDSN(config),
		dbName: database,
	}
}

type mySQLDB struct {
	dsn    string
	db     *sql.DB
	dbName string
}

func (m *mySQLDB) Open() error {
	db, err := sql.Open("mysql", m.dsn)
	if err != nil {
		return err
	}

	m.db = db

	return nil
}

func (m *mySQLDB) Close() error {
	return m.db.Close()
}

func (m *mySQLDB) HasPrimaryKey(tableName string) (bool, error) {
	primaryKey, err := m.GetPrimaryKey(tableName)
	if err != nil {
		return false, err
	}

	return primaryKey != "", nil
}

func (m *mySQLDB) GetPrimaryKey(tableName string) (string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM   INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE  TABLE_SCHEMA = ?
		       AND TABLE_NAME = ?
		       AND CONSTRAINT_NAME = 'PRIMARY'`

	var primaryKey string
	err := m.db.QueryRow(query, m.dbName, tableName).Scan(&primaryKey)
	if err != nil {
		if err == sql.ErrNoRows {
			// No primary key found
			return "", fmt.Errorf("table '%s' has no primary key", tableName)
		} else {
			// Other error occurred
			return "", err
		}
	}

	// Primary key found
	return primaryKey, nil
}

func (m *mySQLDB) GetSchemaRows() (*sql.Rows, error) {
	query := `
	SELECT table_name,
				 column_name,
				 data_type,
				 character_maximum_length
	FROM   information_schema.columns
	WHERE  table_schema = ?`
	rows, err := m.db.Query(query, m.dbName)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (m *mySQLDB) DB() *sql.DB {
	return m.db
}

func (m *mySQLDB) ColumnNameForSelect(name string) string {
	return fmt.Sprintf("`%s`", name)
}

func (m *mySQLDB) EnableConstraints() error {
	_, err := m.db.Exec("SET FOREIGN_KEY_CHECKS = 1;")
	return err
}

func (m *mySQLDB) DisableConstraints() error {
	_, err := m.db.Exec("SET FOREIGN_KEY_CHECKS = 0;")
	return err
}

// Custom define format dsn function so my-sql-driver doesn't add unwanted fields

// FormatDSN formats the given Config into a DSN string which can be passed to
// the driver.
func FormatDSN(cfg mysql.Config) string {
	var buf bytes.Buffer

	// [username[:password]@]
	if len(cfg.User) > 0 {
		buf.WriteString(cfg.User)
		if len(cfg.Passwd) > 0 {
			buf.WriteByte(':')
			buf.WriteString(cfg.Passwd)
		}
		buf.WriteByte('@')
	}

	// [protocol[(address)]]
	if len(cfg.Net) > 0 {
		buf.WriteString(cfg.Net)
		if len(cfg.Addr) > 0 {
			buf.WriteByte('(')
			buf.WriteString(cfg.Addr)
			buf.WriteByte(')')
		}
	}

	// /dbname
	buf.WriteByte('/')
	buf.WriteString(cfg.DBName)

	// [?param1=value1&...&paramN=valueN]
	hasParam := false

	// other params
	if cfg.Params != nil {
		var params []string
		for param := range cfg.Params {
			params = append(params, param)
		}
		sort.Strings(params)
		for _, param := range params {
			writeDSNParam(&buf, &hasParam, param, url.QueryEscape(cfg.Params[param]))
		}
	}

	return buf.String()
}

func writeDSNParam(buf *bytes.Buffer, hasParam *bool, name, value string) {
	buf.Grow(1 + len(name) + 1 + len(value))
	if !*hasParam {
		*hasParam = true
		buf.WriteByte('?')
	} else {
		buf.WriteByte('&')
	}
	buf.WriteString(name)
	buf.WriteByte('=')
	buf.WriteString(value)
}
