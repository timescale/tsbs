package inputs

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

const (
	errUnknownFormatFmt = "unknown format: '%s'"
)

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
