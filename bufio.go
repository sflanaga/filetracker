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

// copy of the io.Copy but changed so that the bytes copied could be more
// closely monitored
func MyCopy(dst io.Writer, src io.Reader) (written int64, err error) {
	return thisCopyBuffer(dst, src)
}

// So, you cannot force allocation of a 32K byte buffer allocation on the stack
// so we resort to using a pool... sigh
// Regardless this GREATLY reduced GC load/message, etc
// But, note that io.Copy does a direct src to dst Reader/Writer copy
// that somehow avoids this, BUT I could only get the byte count AFTER
// a whole file was hashed, which made the stat ticker rather wonky
//
// other things looked into:
//
//		thread locals - none really - just context but meant for http request pipelines
//		using goroutine pool in which the full life time of the goroutine could be tapped
//		but found no thread pool like capability for that in go
//		if you allocate a static array [32*1024] instead of make, it escapes to heap
//	I sense the resource pool here might yet be avoided, but this journey ends here.
func thisCopyBuffer(dst io.Writer, src io.Reader) (written int64, err error) {

	bufPtr := byteBufPool.Get().(*[]byte)
	defer func() {
		byteBufPool.Put(bufPtr)
	}()
	buf := (*bufPtr)[:]

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
