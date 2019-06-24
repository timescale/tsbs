package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"github.com/lib/pq"
	"github.com/timescale/tsbs/load"
)

const (
	insertCSI    = `INSERT INTO %s(time,tags_id,%s%s,additional_tags) VALUES %s`
	numExtraCols = 2 // one for json, one for tags_id
)

type syncCSI struct {
	m     map[string]int64
	mutex *sync.RWMutex
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

func subsystemTagsToJSON(tags []string) map[string]interface{} {
	json := map[string]interface{}{}
	for _, t := range tags {
		args := strings.Split(t, "=")
		json[args[0]] = args[1]
	}
	return json
}

func insertTags(db *sql.DB, tagRows [][]string, returnResults bool) map[string]int64 {
	tagCols := tableCols[tagsKey]
	cols := tagCols
	values := make([]string, 0)
	commonTagsLen := len(tagCols)
	if useJSON {
		cols = []string{"tagset"}
		for _, row := range tagRows {
			json := "('{"
			for i, k := range tagCols {
				if i != 0 {
					json += ","
				}
				json += fmt.Sprintf("\"%s\":\"%s\"", k, row[i])
			}
			json += "}')"
			// Replacing empty tags with NULLs.
			json = strings.ReplaceAll(json, `:""`, `:NULL`)
			values = append(values, json)
		}
	} else {
		for _, val := range tagRows {
			row := fmt.Sprintf("('%s')", strings.Join(val[:commonTagsLen], "','"))
			// Replacing empty tags with NULLs.
			row = strings.ReplaceAll(row, "''", "NULL")
			values = append(values, row)
		}
	}
	tx := MustBegin(db)
	defer tx.Commit()

	res, err := tx.Query(fmt.Sprintf(`INSERT INTO tags(%s) VALUES %s ON CONFLICT DO NOTHING RETURNING *`, strings.Join(cols, ","), strings.Join(values, ",")))
	if err != nil {
		panic(err)
	}

	// Results will be used to make a Golang index for faster inserts
	if returnResults {
		resCols, _ := res.Columns()
		resVals := make([]interface{}, len(resCols))
		resValsPtrs := make([]interface{}, len(resCols))
		for i := range resVals {
			resValsPtrs[i] = &resVals[i]
		}
		ret := make(map[string]int64)
		for res.Next() {
			err = res.Scan(resValsPtrs...)
			if err != nil {
				panic(err)
			}

			var key string
			if useJSON {
				decodedTagset := map[string]string{}
				json.Unmarshal(resVals[1].([]byte), &decodedTagset)
				key = decodedTagset[tagCols[0]]
			} else {
				key = fmt.Sprintf("%v", resVals[1])
			}
			ret[key] = resVals[0].(int64)
		}
		res.Close()
		return ret
	}
	return nil
}

// splitTagsAndMetrics takes an array of insertData (sharded by hypertable) and
// divides the tags from data into appropriate slices that can then be used in
// SQL queries to insert into their respective tables. Additionally, it also
// returns the number of metrics (i.e., non-tag fields) for the data processed.
func splitTagsAndMetrics(rows []*insertData, dataCols int) ([][]string, [][]interface{}, uint64) {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	numMetrics := uint64(0)
	commonTagsLen := len(tableCols[tagsKey])

	for _, data := range rows {
		// Split the tags into individual common tags and an extra bit leftover
		// for non-common tags that need to be added separately. For each of
		// the common tags, remove everything after = in the form <label>=<val>
		// since we won't need it.
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}

		var json interface{}
		if len(tags) > commonTagsLen {
			json = subsystemTagsToJSON(strings.Split(tags[commonTagsLen], ","))
		}

		metrics := strings.Split(data.fields, ",")
		numMetrics += uint64(len(metrics) - 1) // 1 field is timestamp

		timeInt, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		ts := time.Unix(0, timeInt)

		// use nil at 2nd position as placeholder for tagKey
		r := make([]interface{}, 3, dataCols)
		r[0], r[1], r[2] = ts, nil, json
		if inTableTag {
			r = append(r, tags[0])
		}
		for _, v := range metrics[1:] {
			if v == "" {
				r = append(r, nil)
				continue
			}

			num, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(err)
			}

			r = append(r, num)
		}

		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags[:commonTagsLen])
	}

	return tagRows, dataRows, numMetrics
}

func (p *processor) processCSI(hypertable string, rows []*insertData) uint64 {
	colLen := len(tableCols[hypertable]) + numExtraCols
	if inTableTag {
		colLen++
	}
	tagRows, dataRows, numMetrics := splitTagsAndMetrics(rows, colLen)

	// Check if any of these tags has yet to be inserted
	newTags := make([][]string, 0, len(rows))
	p.csi.mutex.RLock()
	for _, cols := range tagRows {
		if _, ok := p.csi.m[cols[0]]; !ok {
			newTags = append(newTags, cols)
		}
	}
	p.csi.mutex.RUnlock()
	if len(newTags) > 0 {
		p.csi.mutex.Lock()
		res := insertTags(p.db, newTags, true)
		for k, v := range res {
			p.csi.m[k] = v
		}
		p.csi.mutex.Unlock()
	}

	p.csi.mutex.RLock()
	for i := range dataRows {
		tagKey := tagRows[i][0]
		dataRows[i][1] = p.csi.m[tagKey]
	}
	p.csi.mutex.RUnlock()

	cols := make([]string, 0, colLen)
	cols = append(cols, "time", "tags_id", "additional_tags")
	if inTableTag {
		cols = append(cols, tableCols[tagsKey][0])
	}
	cols = append(cols, tableCols[hypertable]...)

	if forceTextFormat {
		tx := MustBegin(p.db)
		stmt, err := tx.Prepare(pq.CopyIn(hypertable, cols...))
		if err != nil {
			panic(err)
		}

		for _, r := range dataRows {
			stmt.Exec(r...)
		}
		_, err = stmt.Exec()
		if err != nil {
			panic(err)
		}

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	} else {
		rows := pgx.CopyFromRows(dataRows)
		inserted, err := p.pgxConn.CopyFrom(pgx.Identifier{hypertable}, cols, rows)
		if err != nil {
			panic(err)
		}
		if inserted != len(dataRows) {
			fmt.Fprintf(os.Stderr, "Failed to insert all the data! Expected: %d, Got: %d", len(dataRows), inserted)
			os.Exit(1)
		}
	}

	return numMetrics
}

type processor struct {
	db      *sql.DB
	csi     *syncCSI
	pgxConn *pgx.Conn
}

func (p *processor) Init(workerNum int, doLoad bool) {
	if doLoad {
		p.db = MustConnect(driver, getConnectString())
		if hashWorkers {
			p.csi = newSyncCSI()
		} else {
			p.csi = globalSyncCSI
		}
		if !forceTextFormat {
			conn, err := stdlib.AcquireConn(p.db)
			if err != nil {
				panic(err)
			}
			p.pgxConn = conn
		}
	}
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
	}
	if p.pgxConn != nil {
		err := stdlib.ReleaseConn(p.db, p.pgxConn)
		if err != nil {
			panic(err)
		}
	}
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*hypertableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for hypertable, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(hypertable, rows)

			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0
	return metricCnt, uint64(rowCnt)
}
