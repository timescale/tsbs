package source

import (
	"errors"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
)

type DataSource interface {
	NextItem() *targets.Point
}

func NewDataSource(target targets.ImplementedTarget, config *DataSourceConfig) (DataSource, error) {
	if config.Type == FileDataSourceType {
		return newFileDataSource(target, config.File)
	}
	return nil, errors.New(fmt.Sprintf("Only %s is supported for now", FileDataSourceType))
}
