package timescaledb

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CREATE TABLE ... WITH (tsdb.hypertable, ...), which the loader uses to create
// hypertables with the columnstore enabled, first shipped in TimescaleDB 2.20.
const (
	columnstoreMinMajor = 2
	columnstoreMinMinor = 20
)

// PostLoad runs once all data is loaded. With --use-columnstore it converts
// every chunk of the hypertables this load wrote to into the columnstore (the
// hypertables were created with the columnstore settings already, see
// createColumnstoreTableQuery) and then runs ANALYZE, so that queries are measured against the storage
// layout TimescaleDB is deployed with rather than uncompressed rowstore chunks.
//
// It runs after the load has been timed and reported, so the conversion does
// not count towards the reported ingest rate. The time the conversion took is
// logged separately, so the full cost of getting to compressed data is visible.
func (d *dbCreator) PostLoad(dbName string) error {
	if !d.opts.UseColumnstore {
		return nil
	}
	if err := d.opts.validateColumnstore(); err != nil {
		return err
	}

	db := MustConnect(d.driver, d.opts.GetConnectString(dbName))
	defer db.Close()

	if err := checkColumnstoreVersion(db); err != nil {
		return err
	}

	start := time.Now()
	for _, ht := range metricTables() {
		chunks, err := queryStrings(db, "SELECT show_chunks($1::regclass)::text", ht)
		if err != nil {
			return fmt.Errorf("could not list chunks of %s: %v", ht, err)
		}
		log.Printf("converting %d chunks of %s to columnstore", len(chunks), ht)
		for _, chunk := range chunks {
			// convert_to_columnstore is a procedure, so CALL rather than SELECT
			if _, err := db.Exec("CALL convert_to_columnstore($1::regclass)", chunk); err != nil {
				return fmt.Errorf("could not convert chunk %s: %v", chunk, err)
			}
		}
	}
	log.Printf("columnstore conversion took %0.3fsec (not included in the load time above)", time.Since(start).Seconds())

	// The conversion rewrites every chunk, and the loader never analyzes.
	if _, err := db.Exec("ANALYZE"); err != nil {
		return fmt.Errorf("could not analyze: %v", err)
	}
	return nil
}

// validateColumnstore rejects option combinations the columnstore cannot work
// with. PostCreateDB calls it too, so a bad combination fails before the load
// rather than after it.
func (o *LoadingOptions) validateColumnstore() error {
	if !o.UseColumnstore {
		return nil
	}
	if !o.UseHypertable {
		return fmt.Errorf("--use-columnstore requires --use-hypertable; pass --use-columnstore=false to load into plain PostgreSQL")
	}
	if o.ReplicationFactor > 0 {
		// distributed hypertables were removed in TimescaleDB 2.14, before the
		// columnstore syntax the loader relies on existed
		return fmt.Errorf("--use-columnstore cannot be combined with --replication-factor; pass --use-columnstore=false")
	}
	return nil
}

// checkColumnstoreVersion fails unless the installed timescaledb extension is
// new enough for the columnstore syntax the loader uses.
func checkColumnstoreVersion(db *sql.DB) error {
	var version string
	if err := db.QueryRow("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&version); err != nil {
		return fmt.Errorf("could not read timescaledb version: %v", err)
	}
	if !supportsColumnstore(version) {
		return fmt.Errorf("--use-columnstore needs timescaledb >= %d.%d, got %s; pass --use-columnstore=false for older versions", columnstoreMinMajor, columnstoreMinMinor, version)
	}
	return nil
}

// metricTables returns the tables this load wrote to, in a stable order. It
// reads the tableCols cache that PostCreateDB fills, so any other hypertable
// that happens to live in the same database is left alone.
func metricTables() []string {
	var tables []string
	for name := range tableCols {
		if name != tagsKey {
			tables = append(tables, name)
		}
	}
	sort.Strings(tables)
	return tables
}

// createColumnstoreTableQuery builds the CREATE TABLE that creates a
// hypertable partitioned on time with the columnstore enabled. The partition
// column is named explicitly, as TimescaleDB only infers it from 2.23 on. The
// name is used unquoted, as it is for the plain CREATE TABLE.
func createColumnstoreTableQuery(tableName, columnDefs string, chunkTime time.Duration, segmentBy, orderBy string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (%s) WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.chunk_interval = '%d microseconds', "+
			"tsdb.create_default_indexes = false, tsdb.segmentby = %s, tsdb.orderby = %s)",
		tableName, columnDefs, chunkTime.Microseconds(), quoteLiteral(segmentBy), quoteLiteral(orderBy))
}

// supportsColumnstore reports whether a timescaledb extversion such as
// "2.21.3" or "2.20.0-dev" is new enough for the columnstore syntax.
func supportsColumnstore(version string) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, errMajor := strconv.Atoi(parts[0])
	minor, errMinor := strconv.Atoi(parts[1])
	if errMajor != nil || errMinor != nil {
		return false
	}
	return major > columnstoreMinMajor || (major == columnstoreMinMajor && minor >= columnstoreMinMinor)
}

func quoteLiteral(s string) string {
	return "'" + strings.Replace(s, "'", "''", -1) + "'"
}

func queryStrings(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
