package load

import (
	"bufio"
	"os"
)

const (
	defaultReadSize = 4 << 20 // 4 MB
)

// GetBufferedReader returns the buffered Reader that should be used by the file loader
// if no file name is specified a buffer for STDIN is returned
func GetBufferedReader(fileName string) *bufio.Reader {
	if len(fileName) == 0 {
		// Read from STDIN
		return bufio.NewReaderSize(os.Stdin, defaultReadSize)
	}
	// Read from specified file
	file, err := os.Open(fileName)
	if err != nil {
		fatal("cannot open file for read %s: %v", fileName, err)
		return nil
	}
	return bufio.NewReaderSize(file, defaultReadSize)
}
