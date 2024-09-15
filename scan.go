package main

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"flag"
	"fmt"
	"io/fs"
	"log"
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

var hashpool *pond.WorkerPool
var dbpool *pond.WorkerPool

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

var ptrOutputFile *os.File
var mtxOutputFile sync.Mutex
var dbConn *sql.DB
var dbTx *sql.Tx

func safePrintf(format string, a ...any) {
	mtxOutputFile.Lock()
	defer mtxOutputFile.Unlock()
	fmt.Fprintf(ptrOutputFile, format, a...)
}

var totalSize = statticker.NewStat("bytes", statticker.Bytes)
var countFiles = statticker.NewStat("files", statticker.Count)
var countDirs = statticker.NewStat("dir", statticker.Count)
var goroutines = statticker.NewStat("goroutines", statticker.Gauge)
var blocked = statticker.NewStat("blocked", statticker.Gauge)

var countFileTypes = xsync.NewMapOf[fs.FileMode, int]()

var fsFilter = map[string]bool{
	"/proc": true,
	"/dev":  true,
	"/sys":  true,
}

var filestatErrors uint64 = 0
var notDirOrFile uint64 = 0
var filterDirs uint64 = 0
var dirListErrors uint64 = 0

var startTime = time.Now()

func shaWorker(id int, jobs <-chan string) {
	for filename := range jobs {
		sha1, err := computeSHA1(filename)
		if err == nil {
			safePrintf("scanned: %s = %s\n", filename, string(sha1))
		} else {
			fmt.Errorf("sha1 error for file \"%s\" of: %s\n", filename, err.Error())
		}
	}
}

func walkGo(db *sql.DB, debug bool, dir string, limitworkers *semaphore.Weighted, goroutine bool, depth int) {
	if goroutine {
		// we need to release the allocated thread/goroutine if we stop early
		// we only need to do this when we did NOT steal the next directory/task
		// also note that defer DOES work conditionally here because it works at
		// the end of the current function and NOT the current scope

		// goroutines.Add(1)
		// defer goroutines.Add(-1)

		defer limitworkers.Release(1)
		// fmt.Println("thread start")
		// defer fmt.Println("thread done")
	}

	// goofy special filters
	if depth <= 1 {
		if _, ok := fsFilter[dir]; ok {
			atomic.AddUint64(&filterDirs, 1)
			if debug {
				fmt.Fprintf(os.Stderr, "skipping path %s as special\n", dir)
			}
			return
		}
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		atomic.AddUint64(&dirListErrors, 1)
		if debug {
			fmt.Fprintln(os.Stderr, "Error reading directory:", err)
		}
		return
	}

	for _, file := range files {
		var cleanPath = filepath.Join(dir, file.Name())
		if file.IsDir() {
			countDirs.Add(1)

			if limitworkers.TryAcquire(1) {
				go walkGo(dbConn, debug, cleanPath, limitworkers, true, depth+1)
			} else {
				walkGo(dbConn, debug, cleanPath, limitworkers, false, depth+1)
			}
		} else if file.Type().IsRegular() || (fs.ModeIrregular&file.Type() != 0) {
			stats, err_st := file.Info()
			if err_st != nil {
				atomic.AddUint64(&filestatErrors, 1)
				if debug {
					fmt.Fprintln(os.Stderr, "... Error reading file info:", err_st)
				}
				continue
			}
			safePrintf("toscan: %s\n", cleanPath)
			blocked.Add(1)
			modTime := stats.ModTime()
			sz := stats.Size()
			hashpool.Submit(func() {
				goroutines.Add(1)
				sha1, err := computeSHA1(cleanPath)
				fileInfo := FileInfo{
					ScanTimestamp: startTime, // Current timestamp as scan timestamp
					Filename:      cleanPath,
					FileHash:      sha1,
					ModTime:       modTime,
					Size:          sz,
				}

				goroutines.Add(-1)
				if err == nil {
					countFiles.Add(1)
					safePrintf("scanned: %s | %s | %v | %d\n", fileInfo.Filename, fileInfo.FileHash, fileInfo.ModTime, fileInfo.Size)
				} else {
					safePrintf("scanned: %s FaIleD: %s\n", cleanPath, err.Error())
					fmt.Errorf("sha1 error for file \"%s\" of: %s\n", cleanPath, err.Error())
				}
				dbpool.Submit(func() {
					InsertFileInfo(dbConn, fileInfo)
				})
			})
			blocked.Add(-1)
			// uid := getUserId(&stats)
		} else {
			atomic.AddUint64(&notDirOrFile, 1)
			countFileTypes.Compute(file.Type(), func(oldValue int, loaded bool) (newValue int, delete bool) {
				newValue = oldValue + 1
				return
			})

			// _, _ = countFileTypes.LoadOrStore(key, func(value interface{}) interface{} {
			// 	if value == nil {
			// 		return 1
			// 	}
			// 	return value.(V) + 1
			// })
			if debug {
				fmt.Fprintln(os.Stderr, "... skipping file:", cleanPath, " type: ", modeToStringLong(file.Type()))
			}
		}
	}
	// loadUserInfo(user)

}

func main() {

	// start := time.Now()

	var err error

	_rootDir := flag.String("d", ".", "root directory to scan")
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

	ofile, err := os.OpenFile(*outputFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	dbPath := "mydatabase.duckdb"

	// Connect to the database
	dbConn, err = ConnectToDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer dbConn.Close()

	dbTx, err = dbConn.BeginTx(context.Background(), nil)
	if err != nil {
		log.Fatalf("Failed to start transaction on database: %v", err)
	}
	defer dbTx.Commit()

	// Create the table if it doesn't exist
	if err := CreateTable(dbConn); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	hashpool = pond.New(*threadLimit, *threadLimit*4, pond.MinWorkers(*threadLimit))
	dbpool = pond.New(1, *threadLimit*100, pond.MinWorkers(1))

	ptrOutputFile = ofile

	// jobs := make(chan string, *threadLimit*2)
	// for i := 0; i < *threadLimit; i++ {
	// 	go shaWorker(i, jobs)
	// }

	var workerSema = semaphore.NewWeighted(int64(*threadLimit))

	var statList []*statticker.Stat
	statList = append(statList, countFiles)
	statList = append(statList, countDirs)
	statList = append(statList, totalSize)
	statList = append(statList, goroutines)
	statList = append(statList, blocked)

	var ticker *statticker.Ticker
	if ticker_duration.Seconds() != 0 {
		ticker = statticker.NewTicker("stats monitor", *ticker_duration, statList)
		ticker.Start()
	}
	var ctx = context.Background()
	workerSema.Acquire(ctx, 1)
	fmt.Println("scanning dir: ", rootDir)
	walkGo(dbConn, *debug, rootDir, workerSema, true, 0)
	workerSema.Acquire(ctx, int64(*threadLimit))
	hashpool.StopAndWait()
	ticker.Stop()

}

// OVERALL[stats monitor] 390.863  files: 1,058/s, 413,891 dir: 113/s, 44,463 bytes: 1.36GB/s, 533.18GB goroutines: 0
// OVERALL[stats monitor] 372.238  files: 1,113/s, 414,428 dir: 119/s, 44,463 bytes: 1.43GB/s, 533.38GB goroutines: 0

// perl -ne 'if ( s/scanned: (.+) = .+/$1/) { print $_;}' sha1.log | sort -T /dev/shm > scanned

// perl -ne 'if ( s/toscan: (.+)/$1/) { print $_;}' sha1.log | sort -T /dev/shm > toscan

// 8414e1ffd1d5bcc80db9918042a492f65181bfbe