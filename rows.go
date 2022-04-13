package tablestore

import (
	"database/sql/driver"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"io"
)

type rows struct {
	columns   []string
	resultSet tablestore.SQLResultSet
}

func newRows(resultSet tablestore.SQLResultSet) *rows {
	var columns []string
	for _, columnInfo := range resultSet.Columns() {
		columns = append(columns, columnInfo.Name)
	}
	return &rows{
		columns:   columns,
		resultSet: resultSet,
	}
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	if !r.resultSet.HasNext() {
		return io.EOF
	}
	row := r.resultSet.Next()
	for i := range r.columns {
		if i < len(dest) {
			var err error
			switch r.resultSet.Columns()[i].Type {
			case tablestore.ColumnType_STRING:
				dest[i], err = row.GetString(i)
			case tablestore.ColumnType_INTEGER:
				dest[i], err = row.GetInt64(i)
			case tablestore.ColumnType_BOOLEAN:
				dest[i], err = row.GetBool(i)
			case tablestore.ColumnType_DOUBLE:
				dest[i], err = row.GetFloat64(i)
			case tablestore.ColumnType_BINARY:
				dest[i], err = row.GetBytes(i)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
