package source

import (
	"github.com/timescale/tsbs/load"
)

type DataSource interface {
	NextItem() *load.Point
}

func NewDataSource(config *DataSourceConfig) DataSource {
	if config.Type == FileDataSourceType {
		return newFileDataSource(config.File)
	}
	panic("only file data source supported for now")
}
