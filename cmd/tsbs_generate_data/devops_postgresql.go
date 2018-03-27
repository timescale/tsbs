package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
)

var (
	PostgresqlByteString = []byte("postgresl") // heap optimization
	PostgresqlFields     = []LabeledDistributionMaker{
		{[]byte("numbackends"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("xact_commit"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("xact_rollback"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blks_read"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blks_hit"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_returned"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_fetched"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_inserted"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_updated"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_deleted"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("conflicts"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("temp_files"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("temp_bytes"), func() Distribution { return CWD(ND(1024, 1), 0, 1024*1024*1024, 0) }},
		{[]byte("deadlocks"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blk_read_time"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blk_write_time"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
	}
)

type PostgresqlMeasurement struct {
	timestamp     time.Time
	distributions []Distribution
}

func NewPostgresqlMeasurement(start time.Time) *PostgresqlMeasurement {
	distributions := make([]Distribution, len(PostgresqlFields))
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
