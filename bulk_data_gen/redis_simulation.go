package main

import (
	//"math"
	//"math/rand"
	"time"
)

type LabeledDistributionMaker struct {
	Label             []byte
	DistributionMaker func() Distribution
}

var (
	RedisByteString = []byte("redis") // heap optimization

	RedisUptime = []byte("uptime_in_seconds")

	SixteenGB = float64(16 * 1024 * 1024 * 1024)

	RedisFields = []LabeledDistributionMaker{
		{[]byte("total_connections_received"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("expired_keys"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("evicted_keys"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("keyspace_hits"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("keyspace_misses"), func() Distribution { return MWD(ND(50, 1), 0) }},

		{[]byte("instantaneous_ops_per_sec"), func() Distribution { return WD(ND(1, 1), 0) }},
		{[]byte("instantaneous_input_kbps"), func() Distribution { return WD(ND(1, 1), 0) }},
		{[]byte("instantaneous_output_kbps"), func() Distribution { return WD(ND(1, 1), 0) }},
		{[]byte("connected_clients"), func() Distribution { return CWD(ND(50, 1), 0, 10000, 0) }},
		{[]byte("used_memory"), func() Distribution { return CWD(ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("used_memory_rss"), func() Distribution { return CWD(ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("used_memory_peak"), func() Distribution { return CWD(ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("used_memory_lua"), func() Distribution { return CWD(ND(50, 1), 0, SixteenGB, SixteenGB/2) }},
		{[]byte("rdb_changes_since_last_save"), func() Distribution { return CWD(ND(50, 1), 0, 10000, 0) }},

		{[]byte("sync_full"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("sync_partial_ok"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("sync_partial_err"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("pubsub_channels"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("pubsub_patterns"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("latest_fork_usec"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("connected_slaves"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("master_repl_offset"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("repl_backlog_active"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("repl_backlog_size"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("repl_backlog_histlen"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("mem_fragmentation_ratio"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("used_cpu_sys"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("used_cpu_user"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("used_cpu_sys_children"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("used_cpu_user_children"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
	}
)

type RedisMeasurement struct {
	timestamp time.Time

	uptime        time.Duration
	distributions []Distribution
}

func NewRedisMeasurement(start time.Time) *RedisMeasurement {
	distributions := make([]Distribution, len(RedisFields))
	for i := range RedisFields {
		distributions[i] = RedisFields[i].DistributionMaker()
	}

	return &RedisMeasurement{
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

func (m *RedisMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(RedisByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendField(RedisUptime, int64(m.uptime.Seconds()))
	for i := range m.distributions {
		p.AppendField(RedisFields[i].Label, int64(m.distributions[i].Get()))
	}
}
