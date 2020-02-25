package source

import (
	"errors"
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/targets"
	"strings"
)

type FileDataSourceConfig struct {
	Location string `yaml:"location"`
	Format   string `yaml:"format"`
}

func (f *FileDataSourceConfig) Validate() error {
	if f.Location == "" {
		return errors.New("location of file data source config can't be empty or missing")
	}
	if !utils.IsIn(f.Format, targets.SupportedFormats()) {
		errStr := fmt.Sprintf("file data source format %s not in supported %s",
			f.Format,
			strings.Join(targets.SupportedFormats(), ","),
		)
		return errors.New(errStr)
	}
	return nil
}
