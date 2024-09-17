package main

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/sflanaga/statticker"
	"golang.org/x/sync/semaphore"
)

var gCalcHash = true
var gHashpool *pond.WorkerPool
var gDbpool *pond.WorkerPool

func computeSHA1(filePath string) (string, error) {
	// Open the file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a SHA1 hash object
	hash := sha1.New()

	// Copy the file's contents into the hash object
	_, err = MyCopy(hash, file)
	if err != nil {
		return "", fmt.Errorf("error copying file contents: %w", err)
	}

	// Get the hash as a byte slice
	hashBytes := hash.Sum(nil)

	// Convert the byte slice to a hexadecimal string
	hexString := fmt.Sprintf("%x", hashBytes)

	return hexString, nil
}

func modeToStringLong(mode fs.FileMode) string {
	if mode.IsDir() {
		return "dir"
	} else if mode.IsRegular() {
		return "file"
	} else if mode&fs.ModeSymlink != 0 {
		return "symlink"
	} else if mode&fs.ModeNamedPipe != 0 {
		return "pipe"
	} else if mode&fs.ModeSocket != 0 {
		return "socket"
	} else if mode&fs.ModeCharDevice != 0 {
		return "char-device"
	} else if mode&fs.ModeDevice != 0 {
		return "device"
	} else if mode&fs.ModeIrregular != 0 {
		return "irregular"
	}
	return "unknown"
}

var gPtrOutputFile *os.File
var gMtxOutputFile sync.Mutex

var gDbi *DbInfo

// var dbConn *sql.DB
// var dbTx *sql.Tx

func safePrintf(format string, a ...any) {
	gMtxOutputFile.Lock()
	defer gMtxOutputFile.Unlock()
	fmt.Fprintf(gPtrOutputFile, format, a...)
}

func getThreadCount() int64 {
	return int64(runtime.NumGoroutine())
}

var gTotalSize = statticker.NewStat("bytes", statticker.Bytes)
var gCountFiles = statticker.NewStat("files", statticker.Count)
var gCountDirs = statticker.NewStat("dir", statticker.Count)
var gWalkers = statticker.NewStat("walk", statticker.Gauge)
var gHashers = statticker.NewStat("hash", statticker.Gauge)
var gThreads = statticker.NewStat("threads", statticker.Gauge).WithExternal(getThreadCount)
var gDbQueue = statticker.NewStat("dbq", statticker.Gauge).WithExternal(func() int64 { return int64(gDbpool.WaitingTasks()) })

var gCountFileTypes = xsync.NewMapOf[fs.FileMode, int]()

var cFsFilter = map[string]bool{
	"/proc": true,
	"/dev":  true,
	"/sys":  true,
}

var gFilestatErrors uint64 = 0
var gNotDirOrFile uint64 = 0
var gFilterDirs uint64 = 0
var gDirListErrors uint64 = 0

var gStartTime = time.Now()

func walkGo(debug bool, dir string, limitworkers *semaphore.Weighted, goroutine bool, depth int) {
	if goroutine {
		gWalkers.Add(1)
		defer gWalkers.Add(-1)
		defer limitworkers.Release(1)
	}

	// goofy special filters
	if depth <= 1 {
		if _, ok := cFsFilter[dir]; ok {
			atomic.AddUint64(&gFilterDirs, 1)
			if debug {
				fmt.Fprintf(os.Stderr, "skipping path %s as special\n", dir)
			}
			return
		}
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		atomic.AddUint64(&gDirListErrors, 1)
		if debug {
			fmt.Fprintln(os.Stderr, "Error reading directory:", err)
		}
		return
	}

	for _, file := range files {
		var cleanPath = filepath.Join(dir, file.Name())
		if file.IsDir() {
			gCountDirs.Add(1)

			if limitworkers.TryAcquire(1) {
				go walkGo(debug, cleanPath, limitworkers, true, depth+1)
			} else {
				walkGo(debug, cleanPath, limitworkers, false, depth+1)
			}
		} else if file.Type().IsRegular() || (fs.ModeIrregular&file.Type() != 0) {
			stats, err_st := file.Info()
			if err_st != nil {
				atomic.AddUint64(&gFilestatErrors, 1)
				if debug {
					fmt.Fprintln(os.Stderr, "... Error reading file info:", err_st)
				}
				continue
			}
			gHashers.Add(1)
			modTime := stats.ModTime()
			sz := stats.Size()
			gHashpool.Submit(func() {
				fileInfo := fileInfoPool.Get().(*FileInfo)
				fileInfo.Filename = cleanPath
				fileInfo.ModTime = modTime
				fileInfo.Size = sz

				if gCalcHash {
					sha1, err := computeSHA1(cleanPath)
					if err != nil {
						fmt.Errorf("sha1 error for file \"%s\" of: %s\n", cleanPath, err.Error())
					} else {
						fileInfo.FileHash = sha1
					}
				}
				gCountFiles.Add(1)
				gDbpool.Submit(func() {
					InsertFileInfo(gDbi, fileInfo)
				})
			})
			gHashers.Add(-1)
		} else {
			atomic.AddUint64(&gNotDirOrFile, 1)
			gCountFileTypes.Compute(file.Type(), func(oldValue int, loaded bool) (newValue int, delete bool) {
				newValue = oldValue + 1
				return
			})

			if debug {
				fmt.Fprintln(os.Stderr, "... skipping file:", cleanPath, " type: ", modeToStringLong(file.Type()))
			}
		}
	}
}

func main() {
	go func() {
		http.ListenAndServe("localhost:5000", http.DefaultServeMux)
	}()

	// startTime := time.Now()

	var err error

	_rootDir := flag.String("d", ".", "root directory to scan")
	dbPath := flag.String("D", "track_db.duckdb", "path to the duckdb database file")
	_calcHash := *(flag.Bool("H", false, "cut off sha1 calc hash and just scan files"))
	ticker_duration := flag.Duration("i", 1*time.Second, "ticker duration")
	// dumpFullDetails := flag.Bool("D", false, "dump full details")
	// flatUnits := flag.Bool("F", false, "use basic units for size and age - useful for simpler post processing")
	cpuNum := runtime.NumCPU()
	threadLimit := flag.Int("t", cpuNum, "limit number of threads")
	debug := flag.Bool("v", false, "keep intermediate error messages quiet")
	outputFilename := flag.String("f", "sha1.log", "output file to write sha1 per file")

	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS]\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()

	rootDir, err := filepath.Abs(*_rootDir)
	if err != nil {
		fmt.Println("Error getting absolute path:", err)
		return
	}
	gCalcHash = _calcHash

	ofile, err := os.OpenFile(*outputFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	// Connect to the database
	gDbi, err = NewDbInfo(*dbPath)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer gDbi.txn.Commit()
	defer gDbi.conn.Close()

	gHashpool = pond.New(*threadLimit, *threadLimit*4, pond.MinWorkers(*threadLimit))
	gDbpool = pond.New(1, *threadLimit*5000, pond.MinWorkers(1))

	gPtrOutputFile = ofile

	// jobs := make(chan string, *threadLimit*2)
	// for i := 0; i < *threadLimit; i++ {
	// 	go shaWorker(i, jobs)
	// }

	var workerSema = semaphore.NewWeighted(int64(*threadLimit))

	var statList []*statticker.Stat
	statList = append(statList, gCountFiles)
	statList = append(statList, gCountDirs)
	statList = append(statList, gTotalSize)
	statList = append(statList, gWalkers)
	statList = append(statList, gHashers)
	statList = append(statList, gThreads)
	statList = append(statList, gDbQueue)

	var ticker *statticker.Ticker
	if ticker_duration.Seconds() != 0 {
		ticker = statticker.NewTicker("stats monitor", *ticker_duration, statList)
		ticker.Start()
	}
	var ctx = context.Background()
	workerSema.Acquire(ctx, 1)
	fmt.Println("scanning dir: ", rootDir)
	walkGo(*debug, rootDir, workerSema, true, 0)
	workerSema.Acquire(ctx, int64(*threadLimit))
	gHashpool.StopAndWait()
	ticker.Stop()

	// fmt.Printf("%v\n", startTime)
	err = DbFinalizeStats(gDbi, gStartTime, gCountFiles.Get(), gTotalSize.Get(), time.Since(gStartTime))
	if err != nil {
		log.Fatalf("Error while trying to finalize scan stats: %v", err)
	}

}

// OVERALL[stats monitor] 390.863  files: 1,058/s, 413,891 dir: 113/s, 44,463 bytes: 1.36GB/s, 533.18GB goroutines: 0
// OVERALL[stats monitor] 372.238  files: 1,113/s, 414,428 dir: 119/s, 44,463 bytes: 1.43GB/s, 533.38GB goroutines: 0

// perl -ne 'if ( s/scanned: (.+) = .+/$1/) { print $_;}' sha1.log | sort -T /dev/shm > scanned

// perl -ne 'if ( s/toscan: (.+)/$1/) { print $_;}' sha1.log | sort -T /dev/shm > toscan

// 8414e1ffd1d5bcc80db9918042a492f65181bfbe

// 7010666c27c3e1913dea5af14242411fb3831c21
// 30849452bc095e7773f47b7014b8b228f7032bc8
