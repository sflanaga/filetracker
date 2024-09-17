package main

import (
	"errors"
	"io"
	"sync"
)

var byteBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

var errInvalidWrite = errors.New("invalid write result")

func MyCopy(dst io.Writer, src io.Reader) (written int64, err error) {
	return thisCopyBuffer(dst, src)
}

func thisCopyBuffer(dst io.Writer, src io.Reader) (written int64, err error) {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.

	// if wt, ok := src.(io.WriterTo); ok {
	// 	sz, err := wt.WriteTo(dst)
	// 	totalSize.Add(sz)
	// 	return sz, err
	// }

	bufPtr := byteBufPool.Get().(*[]byte)
	defer func() {
		byteBufPool.Put(bufPtr)
	}()
	buf := (*bufPtr)[:]

	// bufAlloc := [32768]byte{}
	// buf := bufAlloc

	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rf, ok := dst.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	// if buf == nil {
	// 	size := 32 * 1024
	// 	if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
	// 		if l.N < 1 {
	// 			size = 1
	// 		} else {
	// 			size = int(l.N)
	// 		}
	// 	}
	// 	buf = make([]byte, size)
	// }
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errInvalidWrite
				}
			}
			written += int64(nw)
			gTotalSize.Add(int64(nw))
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	buf = buf[0:]
	return written, err
}
