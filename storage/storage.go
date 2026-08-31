// / TODO: clean up repeated parsing of offset, length, bytes
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

type Record struct {
	Offset  uint64
	Payload []byte
}

type Log struct {
	dir     string
	logFile *os.File
	counter uint64
	index   map[uint64]int64
}

func LogOpen(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path.Join(dir, "data.log"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	// seek to the end for appending
	_, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	log := &Log{dir: dir, logFile: f, index: make(map[uint64]int64)}
	err = log.buildIndex()

	if err != nil {
		return nil, err
	}

	return log, nil
}

func (l *Log) buildIndex() error {

	var filePos int64 = 0
	for {
		var offsetBytes [8]byte
		_, err := l.logFile.ReadAt(offsetBytes[:], filePos)
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("buildIndex() failed to parse offset: %v", err)
		}

		offset := parseOffset(offsetBytes[:])

		var lengthBytes [4]byte
		_, err = l.logFile.ReadAt(lengthBytes[:], filePos+8)
		if err != nil {
			return fmt.Errorf("buildIndex() failed to parse length: %v", err)
		}
		length := parseLength(lengthBytes[:])

		nextFilePos := filePos + (12 + int64(length))

		// now ensure the payload is there by trying to seek past the payload
		_, err = l.logFile.Seek(nextFilePos, io.SeekStart)
		// log.Printf("next file pos: %d, but got: %d\n", nextFilePos, retNextFilePos)
		if err != nil {
			// invalid payload, stop parsing
			log.Printf("buildIndex() ending file reading before offset=%d\n", offset)
			break
		} else {
			stat, err := l.logFile.Stat()
			if err != nil {
				return err
			}

			if nextFilePos >= stat.Size() {
				// payload length is past the end of the file, invalid
				log.Printf("buildIndex() invalid length: %d. Values is past the end of the file=%d\n", length, stat.Size())
				// don't bother adding this record to the index. Instead truncate file to  filepos = 1 + end of prev record
				if err := l.logFile.Truncate(filePos); err != nil {
					return err
				}
				break
			} else {
				l.index[offset] = filePos
				filePos = nextFilePos

			}
		}
	}

	return nil

}

func (l *Log) Append(payload []byte) (uint64, error) {
	// write format: [offset:8bytes][length:4bytes][payload:{length} bytes]. Both are written as big endian
	offset := l.counter
	l.counter++

	// 4 bytes is the size of the length field
	serialized := make([]byte, 8+4+len(payload))

	binary.BigEndian.PutUint64(serialized[0:8], offset)
	binary.BigEndian.PutUint32(serialized[8:12], uint32(len(payload)))
	if n := copy(serialized[12:], payload); n != len(payload) {
		return 0, fmt.Errorf("Failed to copy payload into serialized\n")
	}

	// always append to the end
	filePos, err := l.logFile.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, err
	}

	_, err = l.logFile.Write(serialized)
	if err != nil {
		return 0, errors.Join(err, fmt.Errorf("Failed to write serialized data"))
	}
	l.index[offset] = filePos
	// log.Printf("wrote %v offset=%d at file pos=%d\n", serialized, offset, filePos)

	return uint64(offset), nil

}

func parseOffset(data []byte) uint64 {
	return uint64(binary.BigEndian.Uint64(data))

}

func parseLength(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	// log.Printf("index: %+v\n", l.index)
	filePos, ok := l.index[offset]
	if !ok {
		return nil, fmt.Errorf("offset does not exist: %d", offset)
	}

	return l.readAt(offset, int64(filePos))
}

func (l *Log) readAt(offset uint64, filePos int64) ([]byte, error) {
	// log.Printf("Read(%d) at file pos=%d\n", offset, filePos)

	// read the offset at filePos
	var offsetBytes [8]byte
	_, err := l.logFile.ReadAt(offsetBytes[:], int64(filePos))
	if err != nil {
		return nil, fmt.Errorf("failed to read offset %d at file pos %d: %v", offset, filePos, err)
	}

	offsetValue := parseOffset(offsetBytes[:])
	if offsetValue != offset {
		return nil, fmt.Errorf("data corruption at file pos %d. got offset %d but want %d", filePos, offsetValue, offset)
	}

	// read the length
	var lengthBytes [4]byte
	_, err = l.logFile.ReadAt(lengthBytes[:], int64(filePos+8))

	if err != nil {
		return nil, fmt.Errorf("failed to read length  at offset %d at file pos %d: %v", offset, filePos, err)
	}

	// then payload
	length := parseLength(lengthBytes[:])
	payload := make([]byte, length)
	_, err = l.logFile.ReadAt(payload, int64(filePos+12))

	return payload, nil
}

func (l *Log) Close() error {
	err := l.logFile.Sync()
	if err != nil {
		return errors.Join(err, fmt.Errorf("Failed to close log"))
	}
	return l.logFile.Close()
}
