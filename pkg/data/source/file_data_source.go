package source

import "github.com/timescale/tsbs/load"

type fileDataSource struct{}

func newFileDataSource(config *FileDataSourceConfig) *fileDataSource {
	return &fileDataSource{}
}

func (f *fileDataSource) NextItem() *load.Point {
	return nil
}
