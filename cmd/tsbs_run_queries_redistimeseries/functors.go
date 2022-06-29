package main

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/query"
	"strings"
)

// specifically for Resultsets: high-cpu-1, high-cpu-all
// we need to take the reply timestamps and re-issue the query now with
// FILTER_BY_TS given the timestamps that passed the first query condition ( the FILTER_BY_VALUE )
func highCpuFilterByTsFunctor(tq *query.RedisTimeSeries, replies [][]interface{}, idx int, commandArgs []string, p *processor, err error) error {
	if len(replies[idx]) > 0 {
		new_query := []string{commandArgs[0], commandArgs[1], "FILTER_BY_TS"}
		first_serie := replies[idx][0]
		serie_datapoints := first_serie.([]interface{})[2].([]interface{})
		if len(serie_datapoints) == 0 {
			if p.opts.debug {
				fmt.Println(fmt.Sprintf("Applying FILTER_BY_VALUE condition returned zero series"))
			}
			return err
		}
		for _, datapointpair := range serie_datapoints {
			datapoint := datapointpair.([]interface{})
			ts := datapoint[0].(int64)
			new_query = append(new_query, fmt.Sprintf("%d", ts))
		}
		new_query = append(new_query, "FILTER")
		for _, arg := range commandArgs[7 : len(commandArgs)-4] {
			new_query = append(new_query, arg)
		}
		if p.opts.debug {
			fmt.Println(fmt.Sprintf("Applying FILTER_BY_TS condition command (%s %s)", string(tq.CommandNames[idx]), strings.Join(new_query, " ")))
		}
		err = inner_cmd_logic(p, tq, idx, replies, new_query)
	} else {
		if p.opts.debug {
			fmt.Println(fmt.Sprintf("Applying FILTER_BY_VALUE condition returned zero series"))
		}
	}
	return err
}
