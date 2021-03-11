package akumuli

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

const (
	placeholderText = "AAAAFFEE"
)

// Serializer writes a series of Point elements into RESP encoded
// buffer.
type Serializer struct {
	book       map[string]uint32
	bookClosed bool
	deferred   []byte
	index      uint32
}

// NewAkumuliSerializer initializes AkumuliSerializer instance.
func NewAkumuliSerializer() *Serializer {
	s := &Serializer{}
	s.book = make(map[string]uint32)
	s.deferred = make([]byte, 0, 4096)
	s.bookClosed = false
	return s
}

// Serialize writes Point data to the given writer, conforming to the
// AKUMULI RESP protocol.  Serializer adds extra data to guide data loader.
// This function writes output that contains binary and text data in RESP format.
func (s *Serializer) Serialize(p *data.Point, w io.Writer) (err error) {
	deferPoint := false

	buf := make([]byte, 0, 1024)
	// Add cue
	const HeaderLength = 8
	buf = append(buf, placeholderText...)
	buf = append(buf, "+"...)

	// Series name
	fieldKeys := p.FieldKeys()
	measurementName := p.MeasurementName()
	for i := 0; i < len(fieldKeys); i++ {
		buf = append(buf, measurementName...)
		buf = append(buf, '.')
		buf = append(buf, fieldKeys[i]...)
		if i+1 < len(fieldKeys) {
			buf = append(buf, '|')
		} else {
			buf = append(buf, ' ')
		}
	}

	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i := 0; i < len(tagKeys); i++ {
		buf = append(buf, ' ')
		buf = append(buf, tagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, tagValues[i].(string)...)
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
	buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixNano(), buf)
	buf = append(buf, '\n')

	// Values
	fieldValues := p.FieldValues()
	buf = append(buf, fmt.Sprintf("*%d\n", len(fieldValues))...)
	for i := 0; i < len(fieldValues); i++ {
		v := fieldValues[i]
		switch v.(type) {
		case int, int64:
			buf = append(buf, ':')
		case float64:
			buf = append(buf, '+')
		}
		buf = serialize.FastFormatAppend(v, buf)
		buf = append(buf, '\n')
	}

	// Update cue
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(buf)))
	binary.LittleEndian.PutUint16(buf[6:HeaderLength], uint16(len(fieldValues)))
	if deferPoint {
		s.deferred = append(s.deferred, buf...)
		return nil
	}
	_, err = w.Write(buf)
	return err
}
