package serialize

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	placeholderText = "AAAAFFEE"
)

// AkumuliSerializer writes a series of Point elements into RESP encoded
// buffer.
type AkumuliSerializer struct {
	book       map[string]uint32
	bookClosed bool
	deferred   []byte
	index      uint32
}

// NewAkumuliSerializer initializes AkumuliSerializer instance.
func NewAkumuliSerializer() *AkumuliSerializer {
	s := &AkumuliSerializer{}
	s.book = make(map[string]uint32)
	s.deferred = make([]byte, 0, 4096)
	s.bookClosed = false
	return s
}

// Serialize writes Point data to the given writer, conforming to the
// AKUMULI RESP protocol.  Serializer adds extra data to guide data loader.
// This function writes output that contains binary and text data in RESP format.
func (s *AkumuliSerializer) Serialize(p *Point, w io.Writer) (err error) {
	deferPoint := false

	buf := make([]byte, 0, 1024)
	// Add cue
	const HeaderLength = 8
	buf = append(buf, placeholderText...)
	buf = append(buf, "+"...)

	// Series name
	for i := 0; i < len(p.fieldKeys); i++ {
		buf = append(buf, p.measurementName...)
		buf = append(buf, '.')
		buf = append(buf, p.fieldKeys[i]...)
		if i+1 < len(p.fieldKeys) {
			buf = append(buf, '|')
		} else {
			buf = append(buf, ' ')
		}
	}

	for i := 0; i < len(p.tagKeys); i++ {
		buf = append(buf, ' ')
		buf = append(buf, p.tagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, p.tagValues[i].(string)...)
	}

	series := string(buf[HeaderLength:])
	if !s.bookClosed {
		// Save point for later
		if id, ok := s.book[series]; ok {
			s.bookClosed = true
			_, err = w.Write(s.deferred)
			if err != nil {
				return err
			}
			buf = buf[:HeaderLength]
			buf = append(buf, fmt.Sprintf(":%d", id)...)
			binary.LittleEndian.PutUint32(buf[:4], id)
		} else {
			// Shortcut
			s.index++
			tmp := make([]byte, 0, 1024)
			tmp = append(tmp, placeholderText...)
			tmp = append(tmp, "*2\n"...)
			tmp = append(tmp, buf[HeaderLength:]...)
			tmp = append(tmp, '\n')
			tmp = append(tmp, fmt.Sprintf(":%d\n", s.index)...)
			s.book[series] = s.index
			// Update cue
			binary.LittleEndian.PutUint16(tmp[4:6], uint16(len(tmp)))
			binary.LittleEndian.PutUint16(tmp[6:HeaderLength], uint16(0))
			binary.LittleEndian.PutUint32(tmp[:4], s.index)
			binary.LittleEndian.PutUint32(buf[:4], s.index)
			_, err = w.Write(tmp)
			if err != nil {
				return err
			}
			deferPoint = true
			buf = buf[:HeaderLength]
			buf = append(buf, fmt.Sprintf(":%d", s.index)...)
		}
	} else {
		// Replace the series name with the value from the book
		if id, ok := s.book[series]; ok {
			buf = buf[:HeaderLength]
			buf = append(buf, fmt.Sprintf(":%d", id)...)
			binary.LittleEndian.PutUint16(buf[4:6], uint16(len(buf)))
			binary.LittleEndian.PutUint16(buf[6:HeaderLength], uint16(0))
			binary.LittleEndian.PutUint32(buf[:4], id)
		} else {
			return errors.New("unexpected series name")
		}
	}

	buf = append(buf, '\n')

	// Timestamp
	buf = append(buf, ':')
	buf = fastFormatAppend(p.timestamp.UTC().UnixNano(), buf)
	buf = append(buf, '\n')

	// Values
	buf = append(buf, fmt.Sprintf("*%d\n", len(p.fieldValues))...)
	for i := 0; i < len(p.fieldValues); i++ {
		v := p.fieldValues[i]
		switch v.(type) {
		case int, int64:
			buf = append(buf, ':')
		case float64:
			buf = append(buf, '+')
		}
		buf = fastFormatAppend(v, buf)
		buf = append(buf, '\n')
	}

	// Update cue
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(buf)))
	binary.LittleEndian.PutUint16(buf[6:HeaderLength], uint16(len(p.fieldValues)))
	if deferPoint {
		s.deferred = append(s.deferred, buf...)
		return nil
	}
	_, err = w.Write(buf)
	return err
}
