package tablestore

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

var params = map[string]string{
	RetryTimes:         "1",
	MaxRetryTime:       "2s",
	ConnectionTimeout:  "3s",
	RequestTimeout:     "4s",
	MaxIdleConnections: "5",
}

func TestParseDSN(t *testing.T) {
	var builder strings.Builder
	builder.WriteString("https://access_key_id:access_key_secret@example.com/instance_name?")
	for name, value := range params {
		builder.WriteString(name)
		builder.WriteRune('=')
		builder.WriteString(value)
		builder.WriteRune('&')
	}

	cfg, err := parseDSN(builder.String())
	assert.NoError(t, err)
	assert.Equal(t, "access_key_id", cfg.accessKeyId)
	assert.Equal(t, "access_key_secret", cfg.accessKeySecret)
	assert.Equal(t, "https://example.com", cfg.endPoint)
	assert.Equal(t, "instance_name", cfg.instanceName)
	assert.Equal(t, uint(1), cfg.otsConfig.RetryTimes)
	assert.Equal(t, 2*time.Second, cfg.otsConfig.MaxRetryTime)
	assert.Equal(t, 3*time.Second, cfg.otsConfig.HTTPTimeout.ConnectionTimeout)
	assert.Equal(t, 4*time.Second, cfg.otsConfig.HTTPTimeout.RequestTimeout)
	assert.Equal(t, 5, cfg.otsConfig.MaxIdleConnections)
}
