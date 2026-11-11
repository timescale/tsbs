package timescaledb

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

// Hypercore's enable_columnstore / convert_to_columnstore API first shipped in
// TimescaleDB 2.18.
const (
	columnstoreMinMajor = 2
	columnstoreMinMinor = 18
)

// PostLoad runs once all data is loaded. With --use-columnstore it converts
// every chunk of the hypertables this load wrote to into the columnstore and
// then runs ANALYZE, so that queries are measured against the storage
// layout TimescaleDB is deployed with rather than uncompressed rowstore chunks.
//
// It runs after the load has been timed and reported, so the conversion does
// not count towards the reported ingest rate.
func (d *dbCreator) PostLoad(dbName string) error {
	if !d.opts.UseColumnstore {
		return nil
	}
	if err := d.opts.validateColumnstore(); err != nil {
		return err
	}

	db := MustConnect(d.driver, d.opts.GetConnectString(dbName))
	defer db.Close()

	var version string
	if err := db.QueryRow("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&version); err != nil {
		return fmt.Errorf("could not read timescaledb version: %v", err)
	}
	if !supportsColumnstore(version) {
		return fmt.Errorf("--use-columnstore needs timescaledb >= %d.%d, got %s", columnstoreMinMajor, columnstoreMinMinor, version)
	}

	for _, ht := range metricTables() {
		if _, err := db.Exec(enableColumnstoreQuery(ht, d.opts.ColumnstoreSegmentBy, d.opts.ColumnstoreOrderBy)); err != nil {
			return fmt.Errorf("could not enable columnstore on %s: %v", ht, err)
		}
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
	if o.UseColumnstore && !o.UseHypertable {
		return fmt.Errorf("--use-columnstore requires --use-hypertable")
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

// enableColumnstoreQuery builds the ALTER TABLE that turns the columnstore on
// for a hypertable. The name is used unquoted, as it is when the loader creates
// the table.
func enableColumnstoreQuery(hypertable, segmentBy, orderBy string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = %s, timescaledb.orderby = %s)",
		hypertable, quoteLiteral(segmentBy), quoteLiteral(orderBy))
}

// supportsColumnstore reports whether a timescaledb extversion such as
// "2.21.3" or "2.18.0-dev" is new enough for the columnstore API.
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
