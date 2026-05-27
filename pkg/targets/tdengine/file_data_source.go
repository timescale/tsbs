package tdengine

import (
	"bufio"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

func newFileDataSource(fileName string) targets.DataSource {
	br := load.GetBufferedReader(fileName)

	return &fileDataSource{scanner: bufio.NewScanner(br)}
}

type fileDataSource struct {
	scanner *bufio.Scanner
	headers *common.GeneratedDataHeaders
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	p := &point{}
	line := d.scanner.Text()
	p.sqlType = line[0]
	switch line[0] {
	case Insert:
		parts := strings.SplitN(line, ",", 4)
		p.subTable = parts[1]
		p.fieldCount, _ = strconv.Atoi(parts[2])
		p.sql = strings.TrimSpace(parts[3])
	case CreateSTable:
		parts := strings.SplitN(line, ",", 4)
		p.superTable = parts[1]
		p.subTable = parts[2]
		p.sql = parts[3]
	case CreateSubTable:
		parts := strings.SplitN(line, ",", 4)
		p.superTable = parts[1]
		p.subTable = parts[2]
		p.sql = parts[3][12:]
	//case Modify:
	//	parts := strings.SplitN(line, ",", 4)
	//	p.superTable = parts[1]
	//	p.subTable = parts[2]
	//	p.sql = parts[3]
	default:
		panic(line)
	}
	return data.NewLoadedPoint(p)
}
