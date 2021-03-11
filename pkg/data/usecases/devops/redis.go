package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"strconv"
	"time"
)

var (
	labelRedis            = []byte("redis") // heap optimization
	labelRedisTagPort     = []byte("port")
	labelRedisTagServer   = []byte("server")
	labelRedisFieldUptime = []byte("uptime_in_seconds")

	sixteenGB = float64(16 * 1024 * 1024 * 1024)

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	redisLowND  = common.ND(5, 1)
	redisHighND = common.ND(50, 1)

	redisFields = []common.LabeledDistributionMaker{
		{Label: []byte("total_connections_received"), DistributionMaker: func() common.Distribution { return common.MWD(redisLowND, 0) }},
		{Label: []byte("expired_keys"), DistributionMaker: func() common.Distribution { return common.MWD(redisHighND, 0) }},
		{Label: []byte("evicted_keys"), DistributionMaker: func() common.Distribution { return common.MWD(redisHighND, 0) }},
		{Label: []byte("keyspace_hits"), DistributionMaker: func() common.Distribution { return common.MWD(redisHighND, 0) }},
		{Label: []byte("keyspace_misses"), DistributionMaker: func() common.Distribution { return common.MWD(redisHighND, 0) }},

		{Label: []byte("instantaneous_ops_per_sec"), DistributionMaker: func() common.Distribution { return common.WD(common.ND(1, 1), 0) }},
		{Label: []byte("instantaneous_input_kbps"), DistributionMaker: func() common.Distribution { return common.WD(common.ND(1, 1), 0) }},
		{Label: []byte("instantaneous_output_kbps"), DistributionMaker: func() common.Distribution { return common.WD(common.ND(1, 1), 0) }},
		{Label: []byte("connected_clients"), DistributionMaker: func() common.Distribution { return common.CWD(redisHighND, 0, 10000, 0) }},
		{Label: []byte("used_memory"), DistributionMaker: func() common.Distribution { return common.CWD(redisHighND, 0, sixteenGB, sixteenGB/2) }},
		{Label: []byte("used_memory_rss"), DistributionMaker: func() common.Distribution { return common.CWD(redisHighND, 0, sixteenGB, sixteenGB/2) }},
		{Label: []byte("used_memory_peak"), DistributionMaker: func() common.Distribution { return common.CWD(redisHighND, 0, sixteenGB, sixteenGB/2) }},
		{Label: []byte("used_memory_lua"), DistributionMaker: func() common.Distribution { return common.CWD(redisHighND, 0, sixteenGB, sixteenGB/2) }},
		{Label: []byte("rdb_changes_since_last_save"), DistributionMaker: func() common.Distribution { return common.CWD(redisHighND, 0, 10000, 0) }},

		{Label: []byte("sync_full"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("sync_partial_ok"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("sync_partial_err"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("pubsub_channels"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("pubsub_patterns"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("latest_fork_usec"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("connected_slaves"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("master_repl_offset"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("repl_backlog_active"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("repl_backlog_size"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("repl_backlog_histlen"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("mem_fragmentation_ratio"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 100, 0) }},
		{Label: []byte("used_cpu_sys"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("used_cpu_user"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("used_cpu_sys_children"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
		{Label: []byte("used_cpu_user_children"), DistributionMaker: func() common.Distribution { return common.CWD(redisLowND, 0, 1000, 0) }},
	}
)

type RedisMeasurement struct {
	*common.SubsystemMeasurement

	port, serverName string
	uptime           time.Duration
}

func NewRedisMeasurement(start time.Time) *RedisMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, redisFields)
	serverName := fmt.Sprintf("redis_%d", rand.Intn(100000))
	port := strconv.FormatInt(rand.Int63n(20000)+1024, 10)
	return &RedisMeasurement{
		SubsystemMeasurement: sub,
		port:                 port,
		serverName:           serverName,
		uptime:               time.Duration(0),
	}
}

func (m *RedisMeasurement) Tick(d time.Duration) {
	m.SubsystemMeasurement.Tick(d)
	m.uptime += d
}

func (m *RedisMeasurement) ToPoint(p *data.Point) {
	p.AppendField(labelRedisFieldUptime, int64(m.uptime.Seconds()))
	m.ToPointAllInt64(p, labelRedis, redisFields)
	p.AppendTag(labelRedisTagPort, m.port)
	p.AppendTag(labelRedisTagServer, m.serverName)
}
