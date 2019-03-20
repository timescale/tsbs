package main

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
)

const tagsKey = "tags"

var tableCols = make(map[string][]string)

type dbCreator struct {
	br      *bufio.Reader
	tags    string
	cols    []string
	connStr string
	connDB  string
}

func (d *dbCreator) Init() {
	d.readDataHeader(d.br)

	// Needed to connect to user's database in order to drop/create db-name database
	re := regexp.MustCompile(`(dbname)=\S*\b`)
	d.connStr = strings.TrimSpace(re.ReplaceAllString(d.connStr, ""))

	if d.connDB != "" {
		d.connStr = fmt.Sprintf("dbname=%s %s", d.connDB, d.connStr)
	}
}

func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, with the first line containing the tags
	// and their names, the second through N-1 line containing the column
	// names, and last line being blank to separate from the data
	i := 0
	for {
		var err error
		var line string
		if i == 0 {
			d.tags, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			d.tags = strings.TrimSpace(d.tags)
		} else {
			line, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				break
			}
			d.cols = append(d.cols, line)
		}
		i++
	}
}

func (d *dbCreator) DBExists(dbName string) bool {
	db := sqlx.MustConnect(dbType, d.connStr)
	defer db.Close()
	r, _ := db.Queryx("SELECT 1 from pg_database WHERE datname = $1", dbName)
	defer r.Close()
	return r.Next()
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := sqlx.MustConnect(dbType, d.connStr)
	defer db.Close()
	db.MustExec("DROP DATABASE IF EXISTS " + dbName)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	db := sqlx.MustConnect(dbType, d.connStr)
	db.MustExec("CREATE DATABASE " + dbName)
	db.Close()
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	if !createMetricsTable {
		return nil
	}

	dbBench := sqlx.MustConnect(dbType, getConnectString())
	defer dbBench.Close()

	parts := strings.Split(strings.TrimSpace(d.tags), ",")
	if parts[0] != tagsKey {
		return fmt.Errorf("input header in wrong format. got '%s', expected 'tags'", parts[0])
	}
	createTagsTable(dbBench, parts[1:])
	tableCols[tagsKey] = parts[1:]

	for _, cols := range d.cols {
		parts = strings.Split(strings.TrimSpace(cols), ",")
		hypertable := parts[0]
		partitioningField := tableCols[tagsKey][0]
		tableCols[hypertable] = parts[1:]

		pseudoCols := []string{}
		if inTableTag {
			pseudoCols = append(pseudoCols, partitioningField)
		}

		fieldDef := []string{}
		indexes := []string{}
		pseudoCols = append(pseudoCols, parts[1:]...)
		extraCols := 0 // set to 1 when hostname is kept in-table
		for idx, field := range pseudoCols {
			if len(field) == 0 {
				continue
			}
			fieldType := "DOUBLE PRECISION"
			idxType := fieldIndex
			if inTableTag && idx == 0 {
				fieldType = "TEXT"
				idxType = ""
				extraCols = 1
			}

			fieldDef = append(fieldDef, fmt.Sprintf("%s %s", field, fieldType))
			if fieldIndexCount == -1 || idx < (fieldIndexCount+extraCols) {
				indexes = append(indexes, d.getCreateIndexOnFieldCmds(hypertable, field, idxType)...)
			}
		}
		dbBench.MustExec(fmt.Sprintf("DROP TABLE IF EXISTS %s", hypertable))
		dbBench.MustExec(fmt.Sprintf("CREATE TABLE %s (time timestamptz, tags_id integer, %s, additional_tags JSONB DEFAULT NULL)", hypertable, strings.Join(fieldDef, ",")))
		if partitionIndex {
			dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", hypertable))
		}

		// Only allow one or the other, it's probably never right to have both.
		// Experimentation suggests (so far) that for 100k devices it is better to
		// use --time-partition-index for reduced index lock contention.
		if timePartitionIndex {
			dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC, tags_id)", hypertable))
		} else if timeIndex {
			dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", hypertable))
		}

		for _, idxDef := range indexes {
			dbBench.MustExec(idxDef)
		}

		if useHypertable {
			dbBench.MustExec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
			dbBench.MustExec(
				fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, partitioning_column => '%s'::name, number_partitions => %v::smallint, chunk_time_interval => %d, create_default_indexes=>FALSE)",
					hypertable, "tags_id", numberPartitions, chunkTime.Nanoseconds()/1000))
		}
	}
	return nil
}

func createTagsTable(db *sqlx.DB, tags []string) {
	db.MustExec("DROP TABLE IF EXISTS tags")
	if useJSON {
		db.MustExec("CREATE TABLE tags(id SERIAL PRIMARY KEY, tagset JSONB)")
		db.MustExec("CREATE UNIQUE INDEX uniq1 ON tags(tagset)")
		db.MustExec("CREATE INDEX idxginp ON tags USING gin (tagset jsonb_path_ops);")
	} else {
		cols := strings.Join(tags, " TEXT, ")
		cols += " TEXT"
		db.MustExec(fmt.Sprintf("CREATE TABLE tags(id SERIAL PRIMARY KEY, %s)", cols))
		db.MustExec(fmt.Sprintf("CREATE UNIQUE INDEX uniq1 ON tags(%s)", strings.Join(tags, ",")))
		db.MustExec(fmt.Sprintf("CREATE INDEX ON tags(%s)", tags[0]))
	}
}

func (d *dbCreator) getCreateIndexOnFieldCmds(hypertable, field, idxType string) []string {
	ret := []string{}
	for _, idx := range strings.Split(idxType, ",") {
		if idx == "" {
			continue
		}

		indexDef := ""
		if idx == timeValueIdx {
			indexDef = fmt.Sprintf("(time DESC, %s)", field)
		} else if idx == valueTimeIdx {
			indexDef = fmt.Sprintf("(%s, time DESC)", field)
		} else {
			fatal("Unknown index type %v", idx)
		}

		ret = append(ret, fmt.Sprintf("CREATE INDEX ON %s %s", hypertable, indexDef))
	}
	return ret
}
