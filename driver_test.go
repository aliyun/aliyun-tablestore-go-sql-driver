package tablestore

import (
	"database/sql"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/stretchr/testify/suite"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type DriverTestSuite struct {
	suite.Suite
	endpoint        string
	instanceName    string
	accessKeyId     string
	accessKeySecret string
	tableName       string
	url             string
	client          *tablestore.TableStoreClient
}

func (s *DriverTestSuite) SetupSuite() {
	s.endpoint = os.Getenv("OTS_ENDPOINT")
	s.instanceName = os.Getenv("OTS_INSTANCE_NAME")
	s.accessKeyId = os.Getenv("OTS_ACCESS_KEY_ID")
	s.accessKeySecret = os.Getenv("OTS_ACCESS_KEY_SECRET")
	s.tableName = "DriverTestSuite"
	if strings.HasPrefix(s.endpoint, "http://") {
		s.url = fmt.Sprintf("http://%s:%s@%s/%s", s.accessKeyId, s.accessKeySecret, s.endpoint[7:], s.instanceName)
	} else if strings.HasPrefix(s.endpoint, "https://") {
		s.url = fmt.Sprintf("https://%s:%s@%s/%s", s.accessKeyId, s.accessKeySecret, s.endpoint[8:], s.instanceName)
	} else {
		s.Fail("invalid endpoint")
	}
	s.client = tablestore.NewClient(s.endpoint, s.instanceName, s.accessKeyId, s.accessKeySecret)

	// create table
	s.dropTableIfExists()
	tableMeta := &tablestore.TableMeta{TableName: s.tableName}
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_INTEGER)
	tableMeta.AddPrimaryKeyColumn("pk2", tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk3", tablestore.PrimaryKeyType_BINARY)
	tableMeta.AddDefinedColumn("col1", tablestore.DefinedColumn_INTEGER)
	tableMeta.AddDefinedColumn("col2", tablestore.DefinedColumn_DOUBLE)
	tableMeta.AddDefinedColumn("col3", tablestore.DefinedColumn_STRING)
	tableMeta.AddDefinedColumn("col4", tablestore.DefinedColumn_BINARY)
	tableMeta.AddDefinedColumn("col5", tablestore.DefinedColumn_BOOLEAN)
	var indexMetas []*tablestore.IndexMeta
	indexMeta := &tablestore.IndexMeta{IndexName: s.tableName + "_col1_index"}
	indexMeta.AddPrimaryKeyColumn("col1")
	indexMeta.AddDefinedColumn("col2")
	indexMetas = append(indexMetas, indexMeta)
	indexMeta = &tablestore.IndexMeta{IndexName: s.tableName + "_col3_index"}
	indexMeta.AddPrimaryKeyColumn("col3")
	indexMeta.AddDefinedColumn("col4")
	indexMetas = append(indexMetas, indexMeta)
	_, err := s.client.CreateTable(&tablestore.CreateTableRequest{
		TableMeta:          tableMeta,
		TableOption:        &tablestore.TableOption{TimeToAlive: -1, MaxVersion: 1},
		ReservedThroughput: &tablestore.ReservedThroughput{},
	})
	s.NoError(err)

	// create sql binding
	_, err = s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: "CREATE TABLE IF NOT EXISTS " + s.tableName + "(" +
		"pk1 BIGINT," +
		"pk2 VARCHAR(1024)," +
		"pk3 VARBINARY(1024)," +
		"col1 BIGINT," +
		"col2 DOUBLE," +
		"col3 MEDIUMTEXT," +
		"col4 MEDIUMBLOB," +
		"col5 BOOL," +
		"PRIMARY KEY(pk1, pk2, pk3))"})
	s.NoError(err)
	time.Sleep(time.Second)

	// insert rows
	for i := 1; i <= 3; i++ {
		primaryKey := &tablestore.PrimaryKey{}
		primaryKey.AddPrimaryKeyColumn("pk1", int64(i))
		primaryKey.AddPrimaryKeyColumn("pk2", strconv.Itoa(i))
		primaryKey.AddPrimaryKeyColumn("pk3", []byte(strconv.Itoa(i)))
		change := &tablestore.PutRowChange{TableName: s.tableName, PrimaryKey: primaryKey}
		change.AddColumn("col1", int64(i))
		change.AddColumn("col2", float64(i))
		change.AddColumn("col3", strconv.Itoa(i))
		change.AddColumn("col4", []byte(strconv.Itoa(i)))
		change.AddColumn("col5", i%2 == 1)
		change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		_, err = s.client.PutRow(&tablestore.PutRowRequest{PutRowChange: change})
		s.NoError(err)
	}
}

func (s *DriverTestSuite) TearDownSuite() {
	s.dropTableIfExists()
}

func (s *DriverTestSuite) dropTableIfExists() {
	_, _ = s.client.DeleteTable(&tablestore.DeleteTableRequest{TableName: s.tableName})
}

func (s *DriverTestSuite) TestQuery() {
	c, err := sql.Open("ots", s.url)
	s.NoError(err)
	rows, err := c.Query("SELECT * FROM "+s.tableName+" WHERE pk1 = ?", 3)
	s.NoError(err)
	s.True(rows.Next())

	var pk1, col1 int64
	var pk2, col3 string
	var pk3, col4 []byte
	var col2 float64
	var col5 bool
	s.NoError(rows.Scan(&pk1, &pk2, &pk3, &col1, &col2, &col3, &col4, &col5))
	s.Equal(int64(3), pk1)
	s.Equal("3", pk2)
	s.Equal([]byte("3"), pk3)
	s.Equal(int64(3), col1)
	s.Equal(3.0, col2)
	s.Equal("3", col3)
	s.Equal([]byte("3"), col4)
	s.True(col5)
}

func (s *DriverTestSuite) TestPrepare() {
	c, err := sql.Open("ots", s.url)
	s.NoError(err)
	stmt, err := c.Prepare("SELECT * FROM " + s.tableName + " WHERE pk1 = ?")
	s.NoError(err)
	rows, err := stmt.Query(2)
	s.NoError(err)
	s.True(rows.Next())

	var pk1, col1 int64
	var pk2, col3 string
	var pk3, col4 []byte
	var col2 float64
	var col5 bool
	s.NoError(rows.Scan(&pk1, &pk2, &pk3, &col1, &col2, &col3, &col4, &col5))
	s.Equal(int64(2), pk1)
	s.Equal("2", pk2)
	s.Equal([]byte("2"), pk3)
	s.Equal(int64(2), col1)
	s.Equal(2.0, col2)
	s.Equal("2", col3)
	s.Equal([]byte("2"), col4)
	s.False(col5)

	s.False(rows.Next())
	s.NoError(c.Close())
}

func (s *DriverTestSuite) TestExecute() {
	c, err := sql.Open("ots", s.url)
	s.NoError(err)
	result, err := c.Exec("CREATE TABLE IF NOT EXISTS " + s.tableName + "(pk1 BIGINT PRIMARY KEY )")
	s.NoError(err)
	s.Zero(result.LastInsertId())
	s.Zero(result.RowsAffected())
	s.NoError(c.Close())
}

func (s *DriverTestSuite) TestNotSupported() {
	c, err := sql.Open("ots", s.url)
	s.NoError(err)
	_, err = c.Begin()
	s.ErrorIs(err, ErrNotSupported)
}

func (s *DriverTestSuite) TestPlaceholderCount() {
	c, err := sql.Open("ots", s.url)
	s.NoError(err)
	_, err = c.Query("SELECT * FROM t WHERE pk1 = ?")
	s.ErrorIs(err, ErrPlaceholderCount)
	_, err = c.Exec("UPDATE t SET col1 = 1 WHERE pk1 = ?")
	s.ErrorIs(err, ErrPlaceholderCount)
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(DriverTestSuite))
}
