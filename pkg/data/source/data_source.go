package source

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

type DataSource interface {
	NextItem() *data.LoadedPoint
	Headers() *common.GeneratedDataHeaders
}
