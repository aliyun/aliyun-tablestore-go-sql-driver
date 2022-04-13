# Aliyun Tablestore Go SQL Driver

表格存储面向Go语言`database/sql`包的驱动。

## 安装

通过`go get`命令安装：

```bash
go get github.com/aliyun/aliyun-tablestore-go-sql-driver
```

## 使用

表格存储的Go语言驱动是`database/sql/driver`接口的实现，导入包之后即可使用`database/sql`访问表格存储.

使用 `ots` 作为驱动名称，使用有效的DSN作为`dataSourceName`:

```go
import (
    "database/sql"
    "time"
    _ "github.com/aliyun/aliyun-tablestore-go-sql-driver"
)

// ...

db, err := sql.Open("ots", "https://access_key_id:access_key_secret@endpoint/instance_name")
if err != nil {
    panic(err)
}

rows, err := db.Query("SELECT * FROM trips WHERE trip_id = ?", 1688)
if err != nil {
    panic(err)
}

for rows.Next() {
    var (
        tripId     int64
        duration   float64
        bikeNumber string
        isMember   bool
    )
    err = rows.Scan(&tripId, &duration, &bikeNumber, &isMember)
    if err != nil {
        panic(err)
    }
    fmt.Printf("trip_id: %v, duration: %v, bike_number = %v, is_member = %v\n", tripId, duration, bikeNumber, isMember)
}
```

## DSN（数据源名称）

表格存储的DSN（数据源名称）格式定义如下，其中方括号之内为可选部分：

```
schema://access_key_id:access_key_secret@endpoint/instance_name[?param1=value1&...&paramN=valueN]
```

- `schema`为访问表格存储服务的协议，通常为`https`
- `access_key_id`为表格存储服务的AccessKey ID
- `access_key_secret`为表格存储服务的AccessKey Secret
- `endpoint`为表格存储服务的域名地址
- `instance_name`为表格存储服务的实例名称

另外，DSN中支持传入以下配置项：

- `retryTimes`为重试次数
- `connectionTimeout`为连接超时时间
- `requestTimeout`为请求超时时间
- `maxRetryTime`为最大触发重试时间
- `maxIdleConnections`为最大空闲连接数