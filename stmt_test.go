package tablestore

import (
	"database/sql/driver"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestFindPlaceholders(t *testing.T) {
	assert.Equal(t, []int{26, 36}, findPlaceholders("SELECT * FROM t WHERE a = ? AND b = ?"))
	assert.Equal(t, []int{38}, findPlaceholders("SELECT * FROM t WHERE a = '?' AND b = ?"))
	assert.Equal(t, []int{38}, findPlaceholders("SELECT * FROM t WHERE a = \"?\" AND b = ?"))
	assert.Equal(t, []int{38}, findPlaceholders("SELECT * FROM t WHERE a = `?` AND b = ?"))
	assert.Equal(t, []int{40}, findPlaceholders("SELECT * FROM t WHERE a = `\\`?` AND b = ?"))
}

func TestInterpolateParams(t *testing.T) {
	s := newStmt(nil, "SELECT * FROM t WHERE a = ?")
	assert.Equal(t, 1, s.NumInput())

	query, err := s.interpolateParams([]driver.Value{true})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = true", query)
	query, err = s.interpolateParams([]driver.Value{false})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = false", query)
	query, err = s.interpolateParams([]driver.Value{int32(1688)})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 1688", query)

	query, err = s.interpolateParams([]driver.Value{int64(1688)})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 1688", query)
	query, err = s.interpolateParams([]driver.Value{int64(1688)})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 1688", query)

	query, err = s.interpolateParams([]driver.Value{float32(3.14)})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 3.14", query)
	query, err = s.interpolateParams([]driver.Value{float64(3.1415926)})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 3.1415926", query)

	query, err = s.interpolateParams([]driver.Value{nil})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = NULL", query)

	query, err = s.interpolateParams([]driver.Value{"true; delete from t; select * from a = true"})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'true; delete from t; select * from a = true'", query)
	query, err = s.interpolateParams([]driver.Value{"\"HELLO\""})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = '\"HELLO\"'", query)

	query, err = s.interpolateParams([]driver.Value{[]byte{0x40, 0x41, 0x42, 0x43}})
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = FROM_BASE64('QEFCQw==')", query)

	_, err = s.interpolateParams([]driver.Value{time.Now()})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "tablestore: unsupported date type")
}
