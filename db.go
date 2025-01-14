package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/marcboeker/go-duckdb"
)

func dle(err error, format string) {
	if err != nil {
		log.Fatalf(format, err)
	}
}

type DbInfo struct {
	db  *sql.DB
	txn *sql.Tx
	// stmt   *sql.Stmt
	app    *duckdb.Appender
	scanid int64
}

// create (if needed) a new duckdb and associated schema
func NewDbInfo(dbPath string) (*DbInfo, error) {

	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		log.Fatalf("new connector error: %v\n", err)
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		log.Fatalf("new connect error: %v\n", err)
	}

	db := sql.OpenDB(connector)

	txn, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})
	if err != nil {
		log.Fatalf("begin transaction error: %v\n", err)
	}

	queries := []string{
		`begin transaction`,
		`CREATE SEQUENCE IF NOT EXISTS scan_id_seq START WITH 1`,
		`CREATE SEQUENCE IF NOT EXISTS id_seq START WITH 1`,
		`CREATE TABLE IF NOT EXISTS files (
			scan_id bigint,
			filename TEXT,
			filehash TEXT,
			mod_ts TIMESTAMP,
			size BIGINT
		)`,
		// `ALTER TABLE files ADD COLUMN if not exists id BIGINT DEFAULT nextval('id_seq');`,
		`CREATE TABLE IF NOT EXISTS scans (
			scan_id bigint,
			root_dir text,
			start_scan_ts timestamp,
			count bigint,
			bytes bigint,
			exe_time interval
		)`,
		`commit`,
	}

	for _, sql := range queries {
		_, err = db.Exec(sql)
		if err != nil {
			return nil, fmt.Errorf("while creating schema: %v\nsql: %s", err, sql)
		}
	}

	scanid := int64(-1)
	err = txn.QueryRow(`SELECT nextval('scan_id_seq') AS nextval`).Scan(&scanid)
	if err != nil {
		return nil, fmt.Errorf("while getting next scan id: %v", err)
	}

	// Note the append here GREATLY (10 to 100x maybe) improves insert performance
	// but does not use the auto increment sequence that a regular
	// statement uses
	appender, err := duckdb.NewAppenderFromConn(conn, "", "files")
	if err != nil {
		log.Fatalf("new appender error: %v\n", err)
	}

	info := DbInfo{
		db:     db,
		txn:    txn,
		scanid: scanid,
		app:    appender,
	}

	return &info, nil
}

// we are done so record any final stats and commit that data
// In my own tests, appender does not commit until Flush or Close, but the appender docs say that it commits every 204800 rows
// So, if you start the process and get to 300k files and the process exits early due to an error - nothing should appear in the DB
// see https://duckdb.org/docs/data/appender.html
func DbFinalizeStats(dbi *DbInfo, rootDir string, startTime time.Time, fileCount, byteCount int64, exe_time time.Duration) {
	_, err := dbi.txn.Exec(`insert into scans(scan_id, root_dir, start_scan_ts, count, bytes, exe_time) values($1,$2,$3,$4,$5, $6 * INTERVAL '1 milliseconds')`,
		dbi.scanid, rootDir, startTime, fileCount, byteCount, exe_time.Milliseconds())
	if err != nil {
		log.Fatalf("error inserting final scan row stats: %v", err)
	}
	dle(dbi.app.Flush(), "app flush error %v")
	dle(dbi.app.Close(), "app close error %v")
	dle(dbi.txn.Commit(), "txn commit error %v")
}

func InsertFileInfo(dbi *DbInfo, filename string, filehash string, modtime time.Time, filesize int64) error {
	// fmt.Printf("name: %s, mod: %v\n", fileInfo.Filename, fileInfo.ModTime)
	err := dbi.app.AppendRow(dbi.scanid, filename, filehash, modtime, filesize) //fileInfo.Filename, fileInfo.FileHash, fileInfo.ModTime, fileInfo.Size)
	if err != nil {
		log.Fatalf("row appender error: \"%v\" happened on filename: %s\n", err, filename)
	}
	return err
}
