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
	defaultReadSize = 4 << 20 // 4 MB
)

// GetBufferedReader returns a buffered Reader to be used when reading data or queries.
// If a fileName is given, the input will be read from that file, otherwise it will be read from the fallback Reader.
// If the fileName has a .gz extension, the input will be gzipped.
func GetBufferedReader(fileName string, fallback io.Reader) (*bufio.Reader, error) {
	file, err := fileReader(fileName, fallback)
	if err != nil {
		return nil, fmt.Errorf("cannot open file for read %s: %w", fileName, err)
	}
	return bufio.NewReaderSize(file, defaultReadSize), nil
}

// fileReader returns a reader for the given file name,
// handling gzipped files if the file extensions is gzip,
// or fallback if the file name is empty
func fileReader(fileName string, fallback io.Reader) (io.Reader, error) {
	if len(fileName) == 0 {
		return fallback, nil
	}
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(fileName, ".gz") {
		return file, nil
	}
	return gzip.NewReader(file)
}
