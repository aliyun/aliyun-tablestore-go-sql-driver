package tablestore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type connectionConfig struct {
	endPoint        string
	instanceName    string
	accessKeyId     string
	accessKeySecret string
	otsConfig       *tablestore.TableStoreConfig
}

func parseDSN(dsn string) (*connectionConfig, error) {
	parse, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// parse identity
	cfg := &connectionConfig{}
	cfg.accessKeyId = parse.User.Username()
	cfg.accessKeySecret, _ = parse.User.Password()
	cfg.endPoint = parse.Scheme + "://" + parse.Host
	cfg.instanceName = parse.Path
	if strings.HasPrefix(cfg.instanceName, "/") {
		// remove leading slash
		cfg.instanceName = cfg.instanceName[1:]
	}
	cfg.otsConfig = tablestore.NewDefaultTableStoreConfig()

	// parse options
	if val := parse.Query().Get(RetryTimes); val != "" {
		retryTimes, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, err
		}
		cfg.otsConfig.RetryTimes = uint(retryTimes)
	}
	if val := parse.Query().Get(MaxRetryTime); val != "" {
		cfg.otsConfig.MaxRetryTime, err = time.ParseDuration(val)
		if err != nil {
			return nil, err
		}
	}
	if val := parse.Query().Get(ConnectionTimeout); val != "" {
		cfg.otsConfig.HTTPTimeout.ConnectionTimeout, err = time.ParseDuration(val)
		if err != nil {
			return nil, err
		}
	}
	if val := parse.Query().Get(RequestTimeout); val != "" {
		cfg.otsConfig.HTTPTimeout.RequestTimeout, err = time.ParseDuration(val)
		if err != nil {
			return nil, err
		}
	}
	if val := parse.Query().Get(MaxIdleConnections); val != "" {
		cfg.otsConfig.MaxIdleConnections, err = strconv.Atoi(val)
		if err != nil {
			return nil, err
		}
	}
	return cfg, err
}
