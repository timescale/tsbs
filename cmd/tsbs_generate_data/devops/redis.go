package devops

import (
	"fmt"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

type LabeledDistributionMaker struct {
	Label             []byte
	DistributionMaker func() common.Distribution
}

var (
	RedisByteString = []byte("redis") // heap optimization

	RedisUptime = []byte("uptime_in_seconds")

	SixteenGB = float64(16 * 1024 * 1024 * 1024)

	RedisTags = [][]byte{
		[]byte("port"),
		[]byte("server"),
	}

	RedisFields = []LabeledDistributionMaker{
		{[]byte("total_connections_received"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
		{[]byte("expired_keys"), func() common.Distribution { return common.MWD(common.ND(50, 1), 0) }},
		{[]byte("evicted_keys"), func() common.Distribution { return common.MWD(common.ND(50, 1), 0) }},
		{[]byte("keyspace_hits"), func() common.Distribution { return common.MWD(common.ND(50, 1), 0) }},
		{[]byte("keyspace_misses"), func() common.Distribution { return common.MWD(common.ND(50, 1), 0) }},

		{[]byte("instantaneous_ops_per_sec"), func() common.Distribution { return common.WD(common.ND(1, 1), 0) }},
		{[]byte("instantaneous_input_kbps"), func() common.Distribution { return common.WD(common.ND(1, 1), 0) }},
		{[]byte("instantaneous_output_kbps"), func() common.Distribution { return common.WD(common.ND(1, 1), 0) }},
		{[]byte("connected_clients"), func() common.Distribution { return common.CWD(common.ND(50, 1), 0, 10000, 0) }},
		{[]byte("used_memory"), func() common.Distribution { return common.CWD(common.ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("used_memory_rss"), func() common.Distribution { return common.CWD(common.ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("used_memory_peak"), func() common.Distribution { return common.CWD(common.ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("used_memory_lua"), func() common.Distribution { return common.CWD(common.ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("rdb_changes_since_last_save"), func() common.Distribution { return common.CWD(common.ND(50, 1), 0, 10000, 0) }},

		{[]byte("sync_full"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("sync_partial_ok"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("sync_partial_err"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("pubsub_channels"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("pubsub_patterns"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("latest_fork_usec"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("connected_slaves"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("master_repl_offset"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("repl_backlog_active"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("repl_backlog_size"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("repl_backlog_histlen"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("mem_fragmentation_ratio"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 100, 0) }},
		{[]byte("used_cpu_sys"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("used_cpu_user"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("used_cpu_sys_children"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("used_cpu_user_children"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
	}
)

type RedisMeasurement struct {
	timestamp time.Time

	port, serverName []byte
	uptime           time.Duration
	distributions    []common.Distribution
}

func NewRedisMeasurement(start time.Time) *RedisMeasurement {
	distributions := make([]common.Distribution, len(RedisFields))
	for i := range RedisFields {
		distributions[i] = RedisFields[i].DistributionMaker()
	}

	serverName := []byte(fmt.Sprintf("redis_%d", rand.Intn(100000)))
	port := []byte(fmt.Sprintf("%d", rand.Intn(20000)+1024))
	return &RedisMeasurement{
		port:       port,
		serverName: serverName,

		timestamp:     start,
		uptime:        time.Duration(0),
		distributions: distributions,
	}
}

func (m *RedisMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	m.uptime += d

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *RedisMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(RedisByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(RedisTags[0], m.port)
	p.AppendTag(RedisTags[1], m.serverName)

	p.AppendField(RedisUptime, int64(m.uptime.Seconds()))
	for i := range m.distributions {
		p.AppendField(RedisFields[i].Label, int64(m.distributions[i].Get()))
	}
}
