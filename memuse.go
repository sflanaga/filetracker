package main

import (
	"net/http"
	_ "net/http/pprof"
)

// to use:
// go tool pprof http://localhost:5000/debug/pprof/allocs
// left here cause it was hard to re-find this information before
func setupMemMeasure() {
	go func() {
		http.ListenAndServe("localhost:5000", http.DefaultServeMux)
	}()
}
