package serialize

import (
	"fmt"
	"io"
)

// CassandraSerializer writes a Point in a serialized form for Cassandra
type CassandraSerializer struct{}

// Serialize writes Point data to the given writer, conforming to the
// Cassandra format.
//
// This function writes output that looks like:
// series_double,cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production,usage_guest_nice,2016-01-01,1451606400000000000,38.2431182911542820
//
// Which the loader will decode into a statement that looks like this:
// INSERT INTO series_double(series_id,timestamp_ns,value) VALUES('cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production#usage_guest_nice#2016-01-01', 1451606400000000000, 38.2431182911542820)
func (s *CassandraSerializer) Serialize(p *Point, w io.Writer) (err error) {
	seriesIDPrefix := make([]byte, 0, 256)
	seriesIDPrefix = append(seriesIDPrefix, p.measurementName...)
	for i := 0; i < len(p.tagKeys); i++ {
		seriesIDPrefix = append(seriesIDPrefix, ',')
		seriesIDPrefix = append(seriesIDPrefix, p.tagKeys[i]...)
		seriesIDPrefix = append(seriesIDPrefix, '=')
		seriesIDPrefix = append(seriesIDPrefix, p.tagValues[i]...)
	}

	timestampNanos := p.timestamp.UTC().UnixNano()
	timestampBucket := p.timestamp.UTC().Format("2006-01-02")

	for fieldID := 0; fieldID < len(p.fieldKeys); fieldID++ {
		value := p.fieldValues[fieldID]
		tableName := fmt.Sprintf("series_%s", typeNameForCassandra(value))

		buf := make([]byte, 0, 256)
		buf = append(buf, []byte(tableName)...)
		buf = append(buf, []byte(",")...)
		buf = append(buf, seriesIDPrefix...)
		buf = append(buf, []byte(",")...)
		buf = append(buf, p.fieldKeys[fieldID]...)
		buf = append(buf, []byte(",")...)
		buf = append(buf, []byte(timestampBucket)...)
		buf = append(buf, []byte(",")...)
		buf = append(buf, []byte(fmt.Sprintf("%d,", timestampNanos))...)
		buf = fastFormatAppend(value, buf)

		buf = append(buf, []byte("\n")...)

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
