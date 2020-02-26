package source

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"os"
)

const (
	defaultReadSize = 4 << 20 // 4 MB
)

type fileDataSource struct {
	buffer  *bufio.Reader
	decoder load.PointDecoder
}

func newFileDataSource(target targets.ImplementedTarget, config *FileDataSourceConfig) (*fileDataSource, error) {
	buffer, err := getBuffer(config.Location)
	if err != nil {
		log.Printf("Can't prepare data source")
		return nil, err
	}
	decoder := target.Benchmark().GetPointDecoder(buffer)
	return &fileDataSource{buffer, decoder}, nil
}

func (f *fileDataSource) NextItem() *load.Point {
	return f.decoder.Decode(f.buffer)
}

func getBuffer(fileName string) (*bufio.Reader, error) {
	if len(fileName) <= 0 {
		log.Println("Reading data source from STDIN")
		return bufio.NewReaderSize(os.Stdin, defaultReadSize), nil
	}

	log.Printf("Reading data source from file %s", fileName)
	file, err := os.Open(fileName)
	if err != nil {
		errStr := fmt.Sprintf("Couldn't open file %s; %v", fileName, err)
		return nil, errors.New(errStr)
	}
	return bufio.NewReaderSize(file, defaultReadSize), nil
}
