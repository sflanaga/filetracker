package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// FileInfo holds the file details to be inserted
type FileInfo struct {
	Filename string
	FileHash string
	ModTime  time.Time
	Size     int64
}

var fileInfoPool = sync.Pool{
	New: func() interface{} {
		fi := FileInfo{}
		return &fi
	},
}

type DbInfo struct {
	conn   *sql.DB
	txn    *sql.Tx
	stmt   *sql.Stmt
	scanid int64
}

func NewDbInfo(dbPath string) (*DbInfo, error) {
	var err error
	var conn *sql.DB
	var txn *sql.Tx
	var stmt *sql.Stmt

	conn, err = sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	txn, err = conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})
	if err != nil {
		return nil, err
	}

	queries := []string{
		`CREATE SEQUENCE IF NOT EXISTS scan_id_seq START WITH 1`,
		`CREATE SEQUENCE IF NOT EXISTS id_seq START WITH 1`,
		`CREATE TABLE IF NOT EXISTS files (
			scan_id bigint,
			filename TEXT,
			filehash TEXT,
			mod_ts TIMESTAMP,
			size BIGINT
		)`,
		`ALTER TABLE files ADD COLUMN if not exists id BIGINT DEFAULT nextval('id_seq');`,
		`CREATE TABLE IF NOT EXISTS scans (
			scan_id bigint,
			start_scan_ts timestamp,
			count bigint,
			bytes bigint,
			exe_time interval
		)`,
	}

	for _, sql := range queries {
		_, err = txn.Exec(sql)
		if err != nil {
			return nil, fmt.Errorf("while creating schema: %v\nsql: %s", err, sql)
		}
	}

	scanid := int64(-1)
	err = txn.QueryRow(`SELECT nextval('scan_id_seq') AS nextval`).Scan(&scanid)
	if err != nil {
		return nil, fmt.Errorf("while getting next scan id: %v", err)
	}

	stmt, err = txn.PrepareContext(context.Background(), `INSERT INTO files (scan_id, filename, filehash, mod_ts, size)
	VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("while preparing insert statement: %v", err)
	}

	info := DbInfo{
		conn:   conn,
		txn:    txn,
		stmt:   stmt,
		scanid: scanid,
	}

	return &info, nil
}

func DbFinalizeStats(dbi *DbInfo, startTime time.Time, fileCount, byteCount int64, exe_time time.Duration) error {
	_, err := dbi.txn.Exec(`insert into scans(scan_id, start_scan_ts, count, bytes, exe_time) values($1,$2,$3,$4,$5 * INTERVAL '1 milliseconds')`,
		dbi.scanid, startTime, fileCount, byteCount, exe_time.Milliseconds())
	return err
}

func InsertFileInfo(db *DbInfo, fileInfo *FileInfo) error {
	_, err := db.stmt.Exec(db.scanid, fileInfo.Filename, fileInfo.FileHash, fileInfo.ModTime, fileInfo.Size)
	fileInfoPool.Put(fileInfo)
	return err
}
