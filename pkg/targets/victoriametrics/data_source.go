package victoriametrics

import (
	"bufio"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"log"
)

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (f fileDataSource) NextItem() data.LoadedPoint {
	ok := f.scanner.Scan()
	if !ok && f.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		log.Fatalf("scan error: %v", f.scanner.Err())
	}
	return data.NewLoadedPoint(f.scanner.Bytes())
}

func (f fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

type decoder struct {
	scanner *bufio.Scanner
}
