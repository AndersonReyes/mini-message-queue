// / TODO: clean up repeated parsing of offset, length, bytes
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	utils "github.com/andersonreyes/mini-message-queue/utils"
	"io"
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

	logFile := path.Join(dir, "data.log")
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	// seek to the end for appending
	_, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	log := &Log{dir: dir, logFile: f, index: make(map[uint64]int64)}

	info, err := os.Stat(logFile)
	if err == nil && info.Size() > 0 {
		err = log.buildIndex()
		if err != nil {
			return nil, err
		}
	}

	return log, nil
}

func (l *Log) buildIndex() error {
	stat, err := l.logFile.Stat()
	if err != nil {
		return errors.Join(err, fmt.Errorf("failed to get to get the log file stat"))
	}

	var filePos int64 = 0
	for {

		var header [12]byte
		n, err := l.logFile.ReadAt(header[:], filePos)

		if n != 12 || err != nil {
			if err == io.EOF {
				break
			}
			return errors.Join(err, errors.New("failed to read 12 header bytes"))
		}

		offset := parseOffset(header[:8])
		length := parseLength(header[8:])

		payloadEnd := filePos + (12 + int64(length))
		// utils.Logger.Debug("building index", "offset", offset, "filepos", filePos, "length", length, "payloadEnd", payloadEnd)

		if payloadEnd > stat.Size() {
			utils.Logger.Debug(fmt.Sprintf("Payload of record at offset=%d pos=%d is invalid, ignoreing it", offset, filePos))
			break
		}
		l.index[offset] = filePos

		filePos = payloadEnd
	}
	utils.Logger.Debug("finished building the index")

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
		return 0, fmt.Errorf("failed to copy payload into serialized")
	}

	// always append to the end
	filePos, err := l.logFile.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, err
	}

	n, err := l.logFile.Write(serialized)
	if err != nil || n != len(serialized) {
		return 0, errors.Join(err, fmt.Errorf("failed to write serialized data"))
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
	for k, v := range l.index {
		utils.Logger.Debug("debug index: ", "key", k, "val", v)
	}
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

	if err != nil {
		return nil, err
	}

	return payload, nil
}

func (l *Log) Close() error {
	err := l.Flush()
	if err != nil {
		return errors.Join(err, fmt.Errorf("failed to close log"))
	}
	return l.logFile.Close()
}

func (l *Log) Flush() error {
	err := l.logFile.Sync()
	if err != nil {
		return errors.Join(err, fmt.Errorf("failed to close log"))
	}

	return nil
}
