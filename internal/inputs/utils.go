package inputs

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
)

// Formats supported for generation
const (
	FormatCassandra   = "cassandra"
	FormatClickhouse  = "clickhouse"
	FormatInflux      = "influx"
	FormatMongo       = "mongo"
	FormatSiriDB      = "siridb"
	FormatTimescaleDB = "timescaledb"
	FormatCrateDB 	  = "cratedb"
)

const (
	defaultTimeStart = "2016-01-01T00:00:00Z"
	defaultTimeEnd   = "2016-01-02T00:00:00Z"

	errUnknownFormatFmt = "unknown format: '%s'"
)

var formats = []string{
	FormatCassandra,
	FormatClickhouse,
	FormatInflux,
	FormatMongo,
	FormatSiriDB,
	FormatTimescaleDB,
	FormatCrateDB,
}

func isIn(s string, arr []string) bool {
	for _, x := range arr {
		if s == x {
			return true
		}
	}
	return false
}

const (
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseCPUOnly   = "cpu-only"
	useCaseCPUSingle = "cpu-single"
	useCaseDevops    = "devops"
)

var useCaseChoices = []string{
	useCaseCPUOnly,
	useCaseCPUSingle,
	useCaseDevops,
}

// ParseUTCTime parses a string-represented time of the format 2006-01-02T15:04:05Z07:00
func ParseUTCTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

const defaultWriteSize = 4 << 20 // 4 MB

func getBufferedWriter(filename string, fallback io.Writer) (*bufio.Writer, error) {
	// If filename is given, output should go to a file
	if len(filename) > 0 {
		file, err := os.Create(filename)
		if err != nil {
			return nil, fmt.Errorf("cannot open file for write %s: %v", filename, err)
		}
		return bufio.NewWriterSize(file, defaultWriteSize), nil
	}

	return bufio.NewWriterSize(fallback, defaultWriteSize), nil
}

// validateGroups checks validity of combination groupID and totalGroups
func validateGroups(groupID, totalGroupsNum uint) error {
	if totalGroupsNum == 0 {
		// Need at least one group
		return fmt.Errorf(errTotalGroupsZero)
	}
	if groupID >= totalGroupsNum {
		// Need reasonable groupID
		return fmt.Errorf(errInvalidGroupsFmt, groupID, totalGroupsNum)
	}
	return nil
}
