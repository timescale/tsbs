package main

import (
	"bytes"
	"fmt"
	"io"
	"text/template"
	"time"
)

// ElasticSearchDevops produces ES-specific queries for the devops use case.
type ElasticSearchDevops struct {
	Template       *template.Template
	DayIntervals   TimeIntervals
	WeekIntervals  TimeIntervals
	MonthIntervals TimeIntervals
}

// NewElasticSearchDevops makes an ElasticSearchDevops object ready to generate Queries.
func NewElasticSearchDevops(start, end time.Time) *ElasticSearchDevops {
	if !start.Before(end) {
		panic("bad time order")
	}
	t := template.Must(template.New("esDevopsQuery").Parse(rawEsDevopsQuery))

	return &ElasticSearchDevops{
		Template:       t,
		DayIntervals:   NewTimeIntervals(start, end, 24*time.Hour),
		WeekIntervals:  NewTimeIntervals(start, end, 7*24*time.Hour),
		MonthIntervals: NewTimeIntervals(start, end, 31*24*time.Hour),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *ElasticSearchDevops) Dispatch(i int, q *Query) {
	DevopsDispatch(d, i, q)
}

// AvgCPUUsageDayByHour populates a Query for getting the average CPU usage by hour for a random day.
func (d *ElasticSearchDevops) AvgCPUUsageDayByHour(q *Query) {
	interval := d.DayIntervals.RandChoice()

	body := new(bytes.Buffer)
	mustExecuteTemplate(d.Template, body, esDevopsQueryParams{
		Start: interval.StartString(),
		End: interval.EndString(),
		Interval: "1h",
		Field: "usage_user",
	})

	q.HumanLabel = []byte("Elastic CPU day   by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Elastic CPU day   by 1h: %s", interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/cpu/_search")
	q.Body = body.Bytes()
}

// AvgCPUUsageWeekByHour populates a Query for getting the average CPU usage by hour for a random week.
func (d *ElasticSearchDevops) AvgCPUUsageWeekByHour(q *Query) {
	interval := d.WeekIntervals.RandChoice()

	body := new(bytes.Buffer)
	mustExecuteTemplate(d.Template, body, esDevopsQueryParams{
		Start: interval.StartString(),
		End: interval.EndString(),
		Interval: "1h",
		Field: "usage_user",
	})

	q.HumanLabel = []byte("Elastic CPU week  by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Elastic CPU week  by 1h: %s", interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/cpu/_search")
	q.Body = body.Bytes()
}

// AvgCPUUsageMonthByDay populates a Query for getting the average CPU usage by day for a random month.
func (d *ElasticSearchDevops) AvgCPUUsageMonthByDay(q *Query) {
	interval := d.MonthIntervals.RandChoice()

	body := new(bytes.Buffer)
	mustExecuteTemplate(d.Template, body, esDevopsQueryParams{
		Start: interval.StartString(),
		End: interval.EndString(),
		Interval: "1d",
		Field: "usage_user",
	})

	q.HumanLabel = []byte("Elastic CPU month by 1d")
	q.HumanDescription = []byte(fmt.Sprintf("Elastic CPU month by 1d: %s", interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/cpu/_search")
	q.Body = body.Bytes()
}

// AvgMemAvailableDayByHour populates a Query for getting the average memory available by hour for a random day.
func (d *ElasticSearchDevops) AvgMemAvailableDayByHour(q *Query) {
	interval := d.DayIntervals.RandChoice()

	body := new(bytes.Buffer)
	mustExecuteTemplate(d.Template, body, esDevopsQueryParams{
		Start: interval.StartString(),
		End: interval.EndString(),
		Interval: "1h",
		Field: "available",
	})

	q.HumanLabel = []byte("Elastic mem day   by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Elastic mem day   by 1h: %s", interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/mem/_search")
	q.Body = body.Bytes()
}

// AvgMemAvailableWeekByHour populates a Query for getting the average memory available by hour for a random week.
func (d *ElasticSearchDevops) AvgMemAvailableWeekByHour(q *Query) {
	interval := d.WeekIntervals.RandChoice()

	body := new(bytes.Buffer)
	mustExecuteTemplate(d.Template, body, esDevopsQueryParams{
		Start: interval.StartString(),
		End: interval.EndString(),
		Interval: "1h",
		Field: "available",
	})

	q.HumanLabel = []byte("Elastic mem week  by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Elastic mem week  by 1h: %s", interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/mem/_search")
	q.Body = body.Bytes()
}

// AvgMemAvailableMonthByDay populates a Query for getting the average memory available by day for a random month.
func (d *ElasticSearchDevops) AvgMemAvailableMonthByDay(q *Query) {
	interval := d.MonthIntervals.RandChoice()

	body := new(bytes.Buffer)
	mustExecuteTemplate(d.Template, body, esDevopsQueryParams{
		Start: interval.StartString(),
		End: interval.EndString(),
		Interval: "1d",
		Field: "available",
	})

	q.HumanLabel = []byte("Elastic mem month by 1d")
	q.HumanDescription = []byte(fmt.Sprintf("Elastic mem month by 1d: %s", interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/mem/_search")
	q.Body = body.Bytes()
}

func mustExecuteTemplate(t *template.Template, w io.Writer, params interface{}) {
	err := t.Execute(w, params)
	if err != nil {
		panic(fmt.Sprintf("logic error in executing template: %s", err))
	}
}

type esDevopsQueryParams struct {
	Interval, Start, End, Field string
}

const rawEsDevopsQuery = `
{
  "size" : 0,
  "aggs": {
    "result": {
      "filter": {
        "range": {
          "timestamp": {
            "gte": "{{.Start}}",
            "lt": "{{.End}}"
          }
        }
      },
      "aggs": {
        "result2": {
          "date_histogram": {
            "field": "timestamp",
            "interval": "{{.Interval}}",
            "format": "yyyy-MM-dd-HH"
          },
          "aggs": {
            "avg_of_field": {
              "avg": {
                 "field": "{{.Field}}"
              }
            }
          }
        }
      }
    }
  }
}
`
