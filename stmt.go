package tablestore

import (
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"reflect"
	"strings"
)

type stmt struct {
	query  string
	index  []int
	client *tablestore.TableStoreClient
}

func newStmt(client *tablestore.TableStoreClient, query string) *stmt {
	return &stmt{
		client: client,
		query:  query,
		index:  findPlaceholders(query),
	}
}

// Query executes a query that may return rows, such as a SELECT
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	query, err := s.interpolateParams(args)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: query})
	if err != nil {
		return nil, err
	}
	return newRows(resp.ResultSet), nil
}

// Exec executes a query that doesn't return rows, such as an INSERT
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	query, err := s.interpolateParams(args)
	if err != nil {
		return nil, err
	}
	_, err = s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: query})
	if err != nil {
		return nil, err
	}
	return new(noResult), nil
}

// Close closes the statement.
func (s *stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *stmt) NumInput() int {
	return len(s.index)
}

func (s *stmt) interpolateParams(params []driver.Value) (string, error) {
	if len(s.index) != len(params) {
		return "", ErrPlaceholderCount
	}
	if len(params) == 0 {
		return s.query, nil
	}
	var lastAfterIndex int
	var builder strings.Builder
	for i, v := range params {
		builder.WriteString(s.query[lastAfterIndex:s.index[i]])
		lastAfterIndex = s.index[i] + 1
		if v == nil {
			builder.WriteString("NULL")
		} else {
			switch val := v.(type) {
			case bool, int, int8, int16, int32, int64, float32, float64:
				builder.WriteString(fmt.Sprintf("%v", val))
			case string:
				builder.WriteRune('\'')
				builder.WriteString(strings.Replace(val, "'", "\\'", -1))
				builder.WriteRune('\'')
			case []byte:
				builder.WriteString(fmt.Sprintf("FROM_BASE64('%s')", base64.StdEncoding.EncodeToString(val)))
			default:
				return "", errors.New(fmt.Sprintf("tablestore: unsupported date type `%s`", reflect.TypeOf(v).Name()))
			}
		}
	}
	builder.WriteString(s.query[lastAfterIndex:])
	return builder.String(), nil
}

func findPlaceholders(query string) []int {
	var quote uint8
	var index []int
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\\':
			i++
		case '\'', '"', '`':
			if quote == query[i] {
				quote = 0
			} else {
				quote = query[i]
			}
		case '?':
			if quote == 0 {
				index = append(index, i)
			}
		}
	}
	return index
}
