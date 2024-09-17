package main

/*

Scan a directory tree for regular files
Take and record in duckdb table "files":
	scan_id - new sequence created for each scan
	full path
	last mod time,
	filesize
	sha1 hash - disable is optional
record the final scan stats

Note:
	stats are "ticked" as process runs
	some roots are skipped if the root directory is "/"
*/

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"maps"
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

var cFsFilter = map[string]bool{
	"/proc": true,
	"/dev":  true,
	"/sys":  true,
}

// "nothing wrong with globals" - famouse last words
var gCalcHash = true
var gtickerRootDirs = false
var gHashpool *pond.WorkerPool
var gDbpool *pond.WorkerPool

var gDbi *DbInfo

var gTotalSize = statticker.NewStat("bytes", statticker.Bytes)
var gCountFiles = statticker.NewStat("files", statticker.Count)
var gCountDirs = statticker.NewStat("dir", statticker.Count)
var gWalkers = statticker.NewStat("walk", statticker.Gauge)
var gHashers = statticker.NewStat("hash", statticker.Gauge)
var gThreads = statticker.NewStat("threads", statticker.Gauge).WithExternal(getThreadCount)
var gDbQueue = statticker.NewStat("dbq", statticker.Gauge).WithExternal(func() int64 { return int64(gDbpool.WaitingTasks()) })
var gListRoots = statticker.NewStat("print", statticker.Print).WithPrint(printRootInProgress)
var gCountFileTypes = xsync.NewMapOf[fs.FileMode, int]()

var gFilestatErrors uint64 = 0
var gNotDirOrFile uint64 = 0
var gFilterDirs uint64 = 0
var gDirListErrors uint64 = 0

var gStartTime = time.Now()

var gRootMtx = sync.Mutex{}
var gRootsInProgress = make(map[string]bool)

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

func getThreadCount() int64 {
	return int64(runtime.NumGoroutine())
}

// we walkGo spawns another goroutine
// this records for what directory that happened
// this list is time to what are those root directories
// that happen to get a new thread of execution
// and it can be "interesting" to monitor
// and NOT consistent between runs
func addRootDir(rootDir string) {
	gRootMtx.Lock()
	defer gRootMtx.Unlock()
	gRootsInProgress[rootDir] = true
}
func delRootDir(rootDir string) {
	gRootMtx.Lock()
	defer gRootMtx.Unlock()
	delete(gRootsInProgress, rootDir)
}

func printRootInProgress() {
	gRootMtx.Lock()
	strList := maps.Keys(gRootsInProgress)
	gRootMtx.Unlock()
	for s := range strList {
		println("\t", s)
	}
}

// While the walkGo is multi-threaded, it might be considered overkill given we do hashes by default
// except that the process can run without getting hashes and record all files
// note that the hashing is done in a seperate pool from the walk tree
// and it is this hashing that is the primary bottleneck but ONLY if on.
func walkGo(debug bool, dir string, limitworkers *semaphore.Weighted, goroutine bool, depth int) {
	if goroutine {
		if gtickerRootDirs {
			defer delRootDir(dir)
		}
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

	// we scan for the directories first as that allow
	// other threads to find more work
	// as this thread might get bogged down in hashes
	// or very long directory lists
	// so, yes, we iterate on files twice for that reason
	for _, file := range files {
		if file.IsDir() {
			gCountDirs.Add(1)
			var cleanPath = filepath.Join(dir, file.Name())
			// if we have thread capacity - spawn another goroutine
			// according to the semaphore
			// waitgroups and channel could
			if limitworkers.TryAcquire(1) {
				if gtickerRootDirs {
					addRootDir(cleanPath)
				}
				gWalkers.Add(1)
				go walkGo(debug, cleanPath, limitworkers, true, depth+1)
			} else {
				walkGo(debug, cleanPath, limitworkers, false, depth+1)
			}
		}
	}

	for _, file := range files {
		if file.IsDir() {
			// ignore one second pass
			// we start with dirs first to get the spread of workers active
		} else if file.Type().IsRegular() || (fs.ModeIrregular&file.Type() != 0) {
			var cleanPath = filepath.Join(dir, file.Name())
			stats, err_st := file.Info()
			if err_st != nil {
				atomic.AddUint64(&gFilestatErrors, 1)
				if debug {
					fmt.Fprintln(os.Stderr, "... Error reading file info:", err_st)
				}
				continue
			}
			modTime := stats.ModTime()
			sz := stats.Size()
			gHashpool.Submit(func() {
				gHashers.Add(1)
				defer gHashers.Add(-1)

				var filehash string
				if gCalcHash {
					sha1, err := computeSHA1(cleanPath)
					if err != nil {
						fmt.Errorf("sha1 error for file \"%s\" of: %s\n", cleanPath, err.Error())
					} else {
						filehash = sha1
					}
				} else {
					filehash = ""
					gTotalSize.Add(sz)
				}
				gCountFiles.Add(1)
				gDbpool.Submit(func() {
					InsertFileInfo(gDbi, cleanPath, filehash, modTime, sz)
				})
			})
		} else {
			atomic.AddUint64(&gNotDirOrFile, 1)
			gCountFileTypes.Compute(file.Type(), func(oldValue int, loaded bool) (newValue int, delete bool) {
				newValue = oldValue + 1
				return
			})

			if debug {
				fmt.Fprintln(os.Stderr, "... skipping file:", file, " type: ", modeToStringLong(file.Type()))
			}
		}
	}
}

func main() {
	var err error

	// note these assignments do not happen under flag.Parse below
	_rootDir := flag.String("d", ".", "root directory to scan")
	dbPath := flag.String("D", "track_db.duckdb", "path to the duckdb database file")
	_calcHashOff := flag.Bool("H", false, "disable sha1 calc hash and just scan files")
	ticker_duration := flag.Duration("i", 1*time.Second, "ticker duration")
	cpuNum := runtime.NumCPU()
	threadLimit := flag.Int("t", cpuNum, "limit number of threads")
	debug := flag.Bool("v", false, "keep intermediate error messages quiet")
	memprofhttp := flag.Bool("M", false, "activate the mem profile listener")
	tickerRootDirs := flag.Bool("R", false, "ticker will include root directories still in progress")

	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS]\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()
	gtickerRootDirs = *tickerRootDirs
	if *memprofhttp {
		setupMemMeasure()
	}

	rootDir, err := filepath.Abs(*_rootDir)
	if err != nil {
		fmt.Println("Error getting absolute path:", err)
		return
	}
	gCalcHash = !(*_calcHashOff)

	// Connect to the database
	gDbi, err = NewDbInfo(*dbPath)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	// note that we "commit" the data ONLY upon success completion at
	// the last db call.
	defer gDbi.db.Close()

	gHashpool = pond.New(*threadLimit, *threadLimit*4, pond.MinWorkers(*threadLimit))
	gDbpool = pond.New(1, *threadLimit*5000, pond.MinWorkers(1))

	var workerSema = semaphore.NewWeighted(int64(*threadLimit))

	var statList []*statticker.Stat
	statList = append(statList, gCountFiles)
	statList = append(statList, gCountDirs)
	statList = append(statList, gTotalSize)
	statList = append(statList, gWalkers)
	statList = append(statList, gHashers)
	statList = append(statList, gThreads)
	statList = append(statList, gDbQueue)
	if gtickerRootDirs {
		statList = append(statList, gListRoots)
	}

	var ticker *statticker.Ticker
	if ticker_duration.Seconds() != 0 {
		ticker = statticker.NewTicker("stats monitor", *ticker_duration, statList)
		ticker.Start()
	}
	var ctx = context.Background()
	fmt.Println("scanning dir: ", rootDir)
	workerSema.Acquire(ctx, 1)
	gWalkers.Add(1)
	walkGo(*debug, rootDir, workerSema, true, 0)
	workerSema.Acquire(ctx, int64(*threadLimit))
	gHashpool.StopAndWait()
	ticker.Stop()

	DbFinalizeStats(gDbi, rootDir, gStartTime, gCountFiles.Get(), gTotalSize.Get(), time.Since(gStartTime))
}
