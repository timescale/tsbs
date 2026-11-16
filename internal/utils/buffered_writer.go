package utils

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	defaultWriteSize = 4 << 20 // 4 MB
)

// GetBufferedWriter returns a buffered Writer alongside a potential Closer (if using gzip) to be used when saving data or queries.
// If a fileName is given, the output will be written to that file, otherwise it will be written to the fallback Writer.
// If the fileName has a .gz extension, the output will be gzipped and return a closer that needs to be closed.
func GetBufferedWriter(fileName string, fallback io.Writer) (*bufio.Writer, io.Closer, error) {
	file, closer, err := fileWriter(fileName, fallback)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open file for write %s: %w", fileName, err)
	}
	return bufio.NewWriterSize(file, defaultWriteSize), closer, nil
}

// fileWriter returns a Writer for the given file name,
// handling gzipped files if the file extensions is gzip,
// or fallback if the file name is empty.
func fileWriter(fileName string, fallback io.Writer) (io.Writer, io.Closer, error) {
	if len(fileName) == 0 {
		return fallback, nil, nil
	}
	file, err := os.Create(fileName)
	if err != nil {
		return nil, nil, err
	}
	if !strings.HasSuffix(fileName, ".gz") {
		return file, file, nil
	}
	gzFile := gzip.NewWriter(file)
	return gzFile, gzFile, nil
}
