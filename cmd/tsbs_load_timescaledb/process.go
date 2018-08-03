package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"bitbucket.org/440-labs/tsbs/load"
	"github.com/jmoiron/sqlx"
)

const insertCSI = `INSERT INTO %s(time,tags_id,%s%s,additional_tags) VALUES %s`

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

func subsystemTagsToJSON(tags []string) string {
	json := "'{"
	for i, t := range tags {
		args := strings.Split(t, "=")
		if i > 0 {
			json += ","
		}
		json += fmt.Sprintf("\"%s\": \"%s\"", args[0], args[1])
	}
	json += "}'"
	return json
}

func insertTags(db *sqlx.DB, tagRows [][]string, returnResults bool) map[string]int64 {
	tagCols := tableCols["tags"]
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
			values = append(values, json)
		}
	} else {
		for _, val := range tagRows {
			values = append(values, fmt.Sprintf("('%s')", strings.Join(val[:commonTagsLen], "','")))
		}
	}
	tx := db.MustBegin()
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
			ret[fmt.Sprintf("%v", resVals[1])] = resVals[0].(int64)
		}
		res.Close()
		return ret
	}
	return nil
}

func (p *processor) processCSI(hypertable string, rows []*insertData) uint64 {
	partitionKey := ""
	if inTableTag {
		partitionKey = tableCols["tags"][0] + ","
	}

	hypertableCols := strings.Join(tableCols[hypertable], ",")

	tagRows := make([][]string, 0, len(rows))
	dataRows := make([]string, 0, len(rows))
	ret := uint64(0)
	commonTagsLen := len(tableCols["tags"])
	for _, data := range rows {
		// Split the tags into individual common tags and an extra bit leftover
		// for non-common tags that need to be added separately. For each of
		// the common tags, remove everything after = in the form <label>=<val>
		// since we won't need it.
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}
		json := "NULL"
		if len(tags) > commonTagsLen {
			json = subsystemTagsToJSON(strings.Split(tags[commonTagsLen], ","))
		}

		metrics := strings.Split(data.fields, ",")
		ret += uint64(len(metrics) - 1) // 1 field is timestamp

		timeInt, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		ts := time.Unix(0, timeInt).Format("2006-01-02 15:04:05.999999 -0700")

		// The last arguments in these sprintf's may seem a bit confusing at first, but
		// it does work. We want each value to be surrounded by single quotes (' '), and
		// to be separated by a comma. That means we strings.Join them with "', '", which
		// leaves the first value without a preceding ' and the last with out a trailing ',
		// therefore we put the %s returned by the Join inside of '' to solve the problem
		var r string
		if inTableTag {
			r = fmt.Sprintf("('%s','[REPLACE_CSI]', '%s', '%s', %s)", ts, tags[0], strings.Join(metrics[1:], "', '"), json)
		} else {
			r = fmt.Sprintf("('%s', '[REPLACE_CSI]', '%s', %s)", ts, strings.Join(metrics[1:], "', '"), json)
		}

		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags)
	}

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
	for i, r := range dataRows {
		// TODO -- support more than 10 common tags
		tagKey := tagRows[i][0]
		dataRows[i] = strings.Replace(r, "[REPLACE_CSI]", strconv.FormatInt(p.csi.m[tagKey], 10), 1)
	}
	p.csi.mutex.RUnlock()
	tx := p.db.MustBegin()
	_ = tx.MustExec(fmt.Sprintf(insertCSI, hypertable, partitionKey, hypertableCols, strings.Join(dataRows, ",")))

	err := tx.Commit()
	if err != nil {
		panic(err)
	}

	return ret
}

type processor struct {
	db  *sqlx.DB
	csi *syncCSI
}

func (p *processor) Init(workerNum int, doLoad bool) {
	if doLoad {
		p.db = sqlx.MustConnect(dbType, getConnectString())
		if hashWorkers {
			p.csi = newSyncCSI()
		} else {
			p.csi = globalSyncCSI
		}
	}
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
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
