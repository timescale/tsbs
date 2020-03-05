package timescaledb

import (
	"fmt"
	"github.com/spf13/viper"
	"regexp"
	"strings"
	"time"
)

func parseLoadingOptionsConfig(v *viper.Viper){

}
// Loading option vars:
type LoadingOptions struct {
	PostgresConnect string
	Host            string
	DBname          string
	User            string
	Pass            string
	Port            string
	ConnDB          string
	Driver          string // postgres or pgx

	UseHypertable bool
	LogBatches    bool
	UseJSON       bool
	InTableTag    bool
	HashWorkers   bool

	NumberPartitions int
	ChunkTime        time.Duration

	TimeIndex          bool
	TimePartitionIndex bool
	PartitionIndex     bool
	FieldIndex         string
	FieldIndexCount    int

	ProfileFile          string
	ReplicationStatsFile string

	CreateMetricsTable bool
	ForceTextFormat    bool
	TagColumnTypes     []string
}

func (o *LoadingOptions) GetConnectString() string {
	// User might be passing in host=hostname the connect string out of habit which may override the
	// multi host configuration. Same for dbname= and user=. This sanitizes that.
	re := regexp.MustCompile(`(host|dbname|user)=\S*\b`)
	connectString := strings.TrimSpace(re.ReplaceAllString(o.PostgresConnect, ""))
	connectString = fmt.Sprintf("host=%s dbname=%s user=%s %s", o.Host, o.DBname, o.User, connectString)

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
