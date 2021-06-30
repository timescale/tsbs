package timescaledb

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Loading option vars:
type LoadingOptions struct {
	PostgresConnect string `yaml:"postgres" mapstructure:"postgres"`
	Host            string `yaml:"host"`
	User            string
	Pass            string
	Port            string
	ConnDB          string `yaml:"admin-db-name" mapstructure:"admin-db-name"`

	UseHypertable bool `yaml:"use-hypertable" mapstructure:"use-hypertable"`
	LogBatches    bool `yaml:"log-batches" mapstructure:"log-batches"`
	UseJSON       bool `yaml:"use-jsonb-tags" mapstructure:"use-jsonb-tags"`
	InTableTag    bool `yaml:"in-table-partition-tag" mapstructure:"in-table-partition-tag"`

	NumberPartitions  int           `yaml:"partitions" mapstructure:"partitions"`
	PartitionColumn   string        `yaml:"partition-column" mapstructure:"partition-column"`
	ReplicationFactor int           `yaml:"replication-factor" mapstructure:"replication-factor"`
	ChunkTime         time.Duration `yaml:"chunk-time" mapstructure:"chunk-time"`

	TimeIndex          bool   `yaml:"time-index" mapstructure:"time-index"`
	TimePartitionIndex bool   `yaml:"time-partition-index" mapstructure:"time-partition-index"`
	PartitionIndex     bool   `yaml:"partition-index" mapstructure:"partition-index"`
	FieldIndex         string `yaml:"field-index" mapstructure:"field-index"`
	FieldIndexCount    int    `yaml:"field-index-count" mapstructure:"field-index-count"`

	ProfileFile          string `yaml:"write-profile" mapstructure:"write-profile"`
	ReplicationStatsFile string `yaml:"write-replication-stats" mapstructure:"write-replication-stats"`

	CreateMetricsTable bool     `yaml:"create-metrics-table" mapstructure:"create-metrics-table"`
	ForceTextFormat    bool     `yaml:"force-text-format" mapstructure:"force-text-format"`
	TagColumnTypes     []string `yaml:",omitempty" mapstructure:",omitempty"`
	UseInsert          bool     `yaml:"use-insert" mapstructure:"use-insert"`
}

func (o *LoadingOptions) GetConnectString(dbName string) string {
	// User might be passing in host=hostname the connect string out of habit which may override the
	// multi host configuration. Same for dbname= and user=. This sanitizes that.
	re := regexp.MustCompile(`(host|dbname|user)=\S*\b`)
	connectString := strings.TrimSpace(re.ReplaceAllString(o.PostgresConnect, ""))
	connectString = fmt.Sprintf("host=%s dbname=%s user=%s %s", o.Host, dbName, o.User, connectString)

	// For optional parameters, ensure they exist then interpolate them into the connectString
	if len(o.Port) > 0 {
		connectString = fmt.Sprintf("%s port=%s", connectString, o.Port)
	}
	if len(o.Pass) > 0 {
		connectString = fmt.Sprintf("%s password=%s", connectString, o.Pass)
	}

	if o.ForceTextFormat {
		// we assume we're using pq driver
		connectString = fmt.Sprintf("%s disable_prepared_binary_result=yes binary_parameters=no", connectString)
	}

	return connectString
}
