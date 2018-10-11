package devops

import (
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestRedisMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewRedisMeasurement(now)
	origName := string(m.serverName)
	origPort := string(m.port)
	duration := time.Second
	m.Tick(duration)
	if m.uptime != duration {
		t.Errorf("uptime did not update correctly: got %v want %v", m.uptime, duration)
	}
	if got := string(m.serverName); got != origName {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origName)
	}
	if got := string(m.port); got != origPort {
		t.Errorf("port updated unexpectedly: got %s want %s", got, origPort)
	}
	m.Tick(duration)
	if m.uptime != 2*time.Second {
		t.Errorf("update did not update correctly (2nd tick): got %v want %v", m.uptime, 2*time.Second)
	}
	if got := string(m.serverName); got != origName {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origName)
	}
	if got := string(m.port); got != origPort {
		t.Errorf("port updated unexpectedly: got %s want %s", got, origPort)
	}
}

func TestRedisToPoint(t *testing.T) {
	now := time.Now()
	m := NewRedisMeasurement(now)
	origName := string(m.serverName)
	origPort := string(m.port)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelRedis) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelRedis)
	}

	if got := string(p.GetTagValue(labelRedisTagServer)); got != origName {
		t.Errorf("incorrect tag value for server name: got %s want %s", got, origName)
	}

	if got := string(p.GetTagValue(labelRedisTagPort)); got != origPort {
		t.Errorf("incorrect tag value for port: got %s want %s", got, origPort)
	}

	if got := p.GetFieldValue(labelRedisFieldUptime).(int64); got != int64(duration.Seconds()) {
		t.Errorf("incorrect duration for uptime: got %d want %d", got, int64(duration.Seconds()))
	}

	for _, ldm := range RedisFields {
		if got := p.GetFieldValue(ldm.label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.label)
		}
	}
}
