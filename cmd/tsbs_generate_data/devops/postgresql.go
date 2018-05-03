package devops

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
)

var (
	PostgresqlByteString = []byte("postgresl") // heap optimization
	PostgresqlFields     = []LabeledDistributionMaker{
		{[]byte("numbackends"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("xact_commit"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("xact_rollback"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("blks_read"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("blks_hit"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_returned"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_fetched"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_inserted"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_updated"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_deleted"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("conflicts"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("temp_files"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("temp_bytes"), func() common.Distribution { return common.CWD(common.ND(1024, 1), 0, 1024*1024*1024, 0) }},
		{[]byte("deadlocks"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("blk_read_time"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
		{[]byte("blk_write_time"), func() common.Distribution { return common.CWD(common.ND(5, 1), 0, 1000, 0) }},
	}
)

type PostgresqlMeasurement struct {
	timestamp     time.Time
	distributions []common.Distribution
}

func NewPostgresqlMeasurement(start time.Time) *PostgresqlMeasurement {
	distributions := make([]common.Distribution, len(PostgresqlFields))
	for i := range PostgresqlFields {
		distributions[i] = PostgresqlFields[i].DistributionMaker()
	}

	return &PostgresqlMeasurement{
		timestamp:     start,
		distributions: distributions,
	}
}

func (m *PostgresqlMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *PostgresqlMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(PostgresqlByteString)
	p.SetTimestamp(&m.timestamp)

	for i := range m.distributions {
		p.AppendField(PostgresqlFields[i].Label, int64(m.distributions[i].Get()))
	}
}
