package timescaledb

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// Minimum TimescaleDB versions for the features the loader uses.
var (
	// CREATE TABLE ... WITH (tsdb.hypertable, ...) first shipped in 2.20.
	minVersionCreateTableWith = [2]int{2, 20}
	// The tsdb.direct_compress table option first shipped in 2.29.
	minVersionDirectCompress = [2]int{2, 29}
)

// validateColumnstore rejects option combinations the loader cannot create a
// table for. It runs in PostCreateDB, before any data is loaded. The
// columnstore options only apply to hypertables, so with
// --use-hypertable=false they are ignored rather than rejected.
func (o *LoadingOptions) validateColumnstore() error {
	if o.ReplicationFactor > 0 {
		return fmt.Errorf("--replication-factor is no longer supported: multi-node was removed in TimescaleDB 2.14")
	}
	return nil
}

// checkTimescaleVersion fails early when the installed timescaledb extension
// is too old for the CREATE TABLE statements the loader is about to run.
func (o *LoadingOptions) checkTimescaleVersion(db *sql.DB) error {
	if !o.UseHypertable {
		return nil
	}
	var version string
	if err := db.QueryRow("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&version); err != nil {
		return fmt.Errorf("could not read timescaledb version: %v", err)
	}
	min, feature := o.requiredVersion()
	if !versionAtLeast(version, min) {
		return fmt.Errorf("%s needs timescaledb >= %d.%d, got %s", feature, min[0], min[1], version)
	}
	return nil
}

// requiredVersion returns the minimum timescaledb version for the chosen
// options, and what to tell the user needs it.
func (o *LoadingOptions) requiredVersion() ([2]int, string) {
	if o.UseColumnstore && o.DirectCompress {
		return minVersionDirectCompress, "--columnstore-direct-compress (set it to false for older versions)"
	}
	return minVersionCreateTableWith, "hypertables"
}

// hypertableOptions returns the tsdb.* storage parameters that make the
// CREATE TABLE statement create the hypertable in one go. With
// --use-columnstore it also enables the columnstore, and with
// --columnstore-direct-compress the loader writes straight into it, so no
// conversion is needed after the load.
func (o *LoadingOptions) hypertableOptions() []string {
	opts := []string{
		"tsdb.hypertable",
		"tsdb.partition_column = 'time'",
		fmt.Sprintf("tsdb.chunk_interval = %s", quoteLiteral(fmt.Sprintf("%d microseconds", o.ChunkTime.Microseconds()))),
		"tsdb.create_default_indexes = false",
	}
	if !o.UseColumnstore {
		// CREATE TABLE ... WITH turns the columnstore on by default; keep the
		// rowstore unless asked otherwise.
		return append(opts, "tsdb.columnstore = false")
	}
	// An empty segmentby leaves the choice to TimescaleDB.
	if o.ColumnstoreSegmentBy != "" {
		opts = append(opts, fmt.Sprintf("tsdb.segmentby = %s", quoteLiteral(o.ColumnstoreSegmentBy)))
	}
	if o.ColumnstoreOrderBy != "" {
		opts = append(opts, fmt.Sprintf("tsdb.orderby = %s", quoteLiteral(o.ColumnstoreOrderBy)))
	}
	// Likewise, an empty sparse index list leaves the choice to TimescaleDB.
	if o.ColumnstoreSparseIdx != "" {
		opts = append(opts, fmt.Sprintf("tsdb.sparse_index = %s", quoteLiteral(o.ColumnstoreSparseIdx)))
	}
	if o.DirectCompress {
		opts = append(opts, "tsdb.direct_compress")
	}
	return opts
}

// versionAtLeast reports whether a timescaledb extversion such as "2.21.3" or
// "2.18.0-dev" is at least min (major, minor).
func versionAtLeast(version string, min [2]int) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, errMajor := strconv.Atoi(parts[0])
	minor, errMinor := strconv.Atoi(parts[1])
	if errMajor != nil || errMinor != nil {
		return false
	}
	return major > min[0] || (major == min[0] && minor >= min[1])
}

func quoteLiteral(s string) string {
	return "'" + strings.Replace(s, "'", "''", -1) + "'"
}
