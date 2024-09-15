package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// FileInfo holds the file details to be inserted
type FileInfo struct {
	ScanTimestamp time.Time
	Filename      string
	FileHash      string
	ModTime       time.Time
	Size          int64
}

// ConnectToDB connects to the DuckDB database and returns a database handle
func ConnectToDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// CreateTable creates a table in the DuckDB database if it doesn't exist
func CreateTable(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		scan_timestamp TIMESTAMP,
		filename TEXT,
		filehash TEXT,
		modification_timestamp TIMESTAMP,
		size BIGINT
	)`
	_, err := db.Exec(query)
	return err
}

// CalculateFileHash calculates the SHA-256 hash of the file content
func CalculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// InsertFileInfo inserts file details into the DuckDB database
func InsertFileInfo(db *sql.DB, fileInfo FileInfo) error {
	query := `
	INSERT INTO files (scan_timestamp, filename, filehash, modification_timestamp, size)
	VALUES (?, ?, ?, ?, ?)`
	_, err := db.Exec(query, fileInfo.ScanTimestamp, fileInfo.Filename, fileInfo.FileHash, fileInfo.ModTime, fileInfo.Size)
	return err
}

func main() {
	// Path to your DuckDB database
	dbPath := "mydatabase.duckdb"

	// Connect to the database
	db, err := ConnectToDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Create the table if it doesn't exist
	if err := CreateTable(db); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// File path to process
	filePath := "example.txt"
	info, err := os.Stat(filePath)
	if err != nil {
		log.Fatalf("Failed to get file info: %v", err)
	}

	fileHash, err := CalculateFileHash(filePath)
	if err != nil {
		log.Fatalf("Failed to calculate file hash: %v", err)
	}

	fileInfo := FileInfo{
		ScanTimestamp: time.Now(), // Current timestamp as scan timestamp
		Filename:      filePath,
		FileHash:      fileHash,
		ModTime:       info.ModTime(),
		Size:          info.Size(),
	}

	if err := InsertFileInfo(db, fileInfo); err != nil {
		log.Fatalf("Failed to insert file info: %v", err)
	}

	fmt.Println("File info inserted successfully")
}
