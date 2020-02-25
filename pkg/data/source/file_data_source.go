package source

import (
	"bufio"
	"github.com/timescale/tsbs/load"
)

type fileDataSource struct {
	buffer  *bufio.Reader
	decoder load.PointDecoder
}

func newFileDataSource(config *FileDataSourceConfig) *fileDataSource {
	return &fileDataSource{}
}

func (f *fileDataSource) NextItem() *load.Point {
	return f.decoder.Decode(f.buffer)
}
