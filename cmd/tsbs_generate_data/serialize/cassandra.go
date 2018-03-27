package serialize

import (
	"fmt"
	"io"
)

// CassandraSerializer writes a Point in a serialized form for Cassandra
type CassandraSerializer struct {
	PointSerializer
}

// Serialize writes Point data to the given writer, conforming to the
// Cassandra query format.
//
// This function writes output that looks like:
// INSERT INTO <tablename> (series_id, ts_ns, value) VALUES (<series_id>, <timestamp_nanoseconds>, <field value>)
// where series_id looks like: <measurement>,<tagset>#<field name>#<time shard>
//
// For example:
// INSERT INTO all_series (series_id, timestamp_ns, value) VALUES ('cpu,hostname=host_01#user#2016-01-01', 12345, 42.1)\n
func (s *CassandraSerializer) Serialize(p *Point, w io.Writer) (err error) {
	seriesIDPrefix := make([]byte, 0, 256)
	seriesIDPrefix = append(seriesIDPrefix, p.MeasurementName...)
	for i := 0; i < len(p.TagKeys); i++ {
		seriesIDPrefix = append(seriesIDPrefix, ',')
		seriesIDPrefix = append(seriesIDPrefix, p.TagKeys[i]...)
		seriesIDPrefix = append(seriesIDPrefix, '=')
		seriesIDPrefix = append(seriesIDPrefix, p.TagValues[i]...)
	}

	timestampNanos := p.Timestamp.UTC().UnixNano()
	timestampBucket := p.Timestamp.UTC().Format("2006-01-02")

	for fieldID := 0; fieldID < len(p.FieldKeys); fieldID++ {
		v := p.FieldValues[fieldID]
		tableName := fmt.Sprintf("measurements.series_%s", typeNameForCassandra(v))

		buf := make([]byte, 0, 256)
		buf = append(buf, []byte("INSERT INTO ")...)
		buf = append(buf, []byte(tableName)...)
		buf = append(buf, []byte(" (series_id, timestamp_ns, value) VALUES ('")...)
		buf = append(buf, seriesIDPrefix...)
		buf = append(buf, byte('#'))
		buf = append(buf, p.FieldKeys[fieldID]...)
		buf = append(buf, byte('#'))
		buf = append(buf, []byte(timestampBucket)...)
		buf = append(buf, byte('\''))
		buf = append(buf, ", "...)
		buf = append(buf, []byte(fmt.Sprintf("%d, ", timestampNanos))...)

		buf = fastFormatAppend(v, buf)

		buf = append(buf, []byte(")\n")...)

		_, err := w.Write(buf)
		if err != nil {
			return err
		}
	}

	return nil
}

func typeNameForCassandra(v interface{}) string {
	switch v.(type) {
	case int, int64:
		return "bigint"
	case float64:
		return "double"
	case float32:
		return "float"
	case bool:
		return "boolean"
	case []byte, string:
		return "blob"
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
