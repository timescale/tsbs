package serialize

import (
	"io"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

// PointSerializer serializes a Point for writing
type PointSerializer interface {
	Serialize(p *data.Point, w io.Writer) error
}

type ConfigurableSerializer interface {
	PointSerializer
	Config(*common.DataGeneratorConfig, io.Writer) error
}
