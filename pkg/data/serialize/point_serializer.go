package serialize

import (
	"github.com/timescale/tsbs/pkg/data"
	"io"
)

// PointSerializer serializes a Point for writing
type PointSerializer interface {
	Serialize(p *data.Point, w io.Writer) error
}
