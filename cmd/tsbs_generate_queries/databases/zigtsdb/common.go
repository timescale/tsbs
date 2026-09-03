package zigtsdb

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	iutils "github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// maxStatementBytes keeps every statement under zigtsdb's 2048-byte decoded
// ZQL limit with headroom; a longer statement would be rejected server-side.
const maxStatementBytes = 2000

// BaseGenerator contains settings specific for zigtsdb.
type BaseGenerator struct{}

// GenerateEmptyQuery returns an empty query.HTTP.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)
	if err != nil {
		return nil, err
	}
	return &Devops{
		BaseGenerator: g,
		Core:          core,
	}, nil
}

// fillInQuery packs one or more ZQL statements into a query.HTTP. The first
// statement is encoded in Path (`GET /query?zql=...`); any further ones are
// carried raw, newline-separated, in Body and encoded by the runner. All of
// them are kept in RawQuery for debugging.
func (g *BaseGenerator) fillInQuery(qq query.Query, label, desc string, interval *iutils.TimeInterval, stmts []string) {
	if len(stmts) == 0 {
		panic("BUG: fillInQuery called without statements")
	}
	for _, s := range stmts {
		if len(s) > maxStatementBytes {
			panic(fmt.Sprintf("ZQL statement of %d bytes exceeds the %d-byte limit: %s", len(s), maxStatementBytes, s))
		}
	}
	q := qq.(*query.HTTP)
	q.HumanLabel = []byte(label)
	q.HumanDescription = []byte(desc)
	q.Method = []byte("GET")
	q.Path = []byte("/query?zql=" + encodeZQL(stmts[0]))
	q.Body = []byte(strings.Join(stmts[1:], "\n"))
	q.RawQuery = []byte(strings.Join(stmts, ";\n"))
	if interval != nil {
		q.StartTimestamp = interval.StartUnixNano()
		q.EndTimestamp = interval.EndUnixNano()
	}
}

// encodeZQL percent-encodes a statement; spaces become %20 rather than '+'
// so any RFC 3986 decoder reads it back correctly.
func encodeZQL(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}
