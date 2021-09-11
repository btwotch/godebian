package godebian

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type SqliteDb struct {
	dbPath        string
	inTransaction bool
	baseDB

	setETagStmt                     *stmt
	getETagStmt                     *stmt
	insertPackageFileStmt           *stmt
	getPackageByFilepathVersionStmt *stmt
	getPackageByFilenameVersionStmt *stmt
	getPackages                     *stmt
	removeAllPackagesStmt           *stmt
}

func (db *SqliteDb) Open() {
	var err error

	if db.dbPath == "" {
		dirname, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		db.dbPath = filepath.Join(dirname, ".godebian.sqlite")
	}

	db.db, err = sql.Open("sqlite3", db.dbPath)
	if err != nil {
		panic("Could not open db: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS etag (version VARCHAR, current VARCHAR, PRIMARY KEY(version))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS file2package (version VARCHAR, path VARCHAR, package VARCHAR, PRIMARY KEY(version, path, package))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	db.prepareStatements()

	db.inTransaction = false
}

func (db *SqliteDb) prepareStatements() {
	var stmts = []struct {
		name    string
		stmtStr string
		stmt    **stmt
	}{
		{"set ETag", "INSERT OR REPLACE INTO etag (version, current) VALUES (?, ?)", &db.setETagStmt},
		{"get ETag", "SELECT current FROM etag WHERE version = ?", &db.getETagStmt},
		{"insert package file", "INSERT OR REPLACE INTO file2package (version, path, package) VALUES (?, ?, ?)", &db.insertPackageFileStmt},
		{"get package by version and file path", "SELECT package FROM file2package WHERE version = ? AND path = ?", &db.getPackageByFilepathVersionStmt},
		{"get package by version and file name", "SELECT package FROM file2package WHERE version = ? AND path LIKE ?", &db.getPackageByFilenameVersionStmt},
		{"remove all packages of version", "DELETE FROM file2package WHERE version = ?", &db.removeAllPackagesStmt},
		{"list packages by version", "SELECT path, package FROM file2package WHERE version = ?", &db.getPackages},
	}

	var err error
	for _, s := range stmts {
		stmt := db.newStmt(s.name, s.stmtStr)
		*s.stmt = stmt
		if err != nil {
			panic(s.name + ": could not prepare statement: " + err.Error())
		}

	}

}

func (db *SqliteDb) removeAllPackages(version string) {
	db.removeAllPackagesStmt.Exec(version)
}

func (db *SqliteDb) getPackageByX(version, path string, s *stmt) []string {
	var filePackages []string

	rows := s.Query(version, path)
	defer rows.Close()

	for rows.Next() {
		var filePackage string
		err := rows.Scan(&filePackage)
		if err != nil {
			panic(err)
		}

		filePackages = append(filePackages, filePackage)
	}

	return filePackages
}

func (db *SqliteDb) getPackage(version, path string) []string {
	if strings.HasPrefix(path, "/") {
		return db.getPackageByX(version, path, db.getPackageByFilepathVersionStmt)
	} else {
		path = fmt.Sprintf("%%/%s", path)
		return db.getPackageByX(version, path, db.getPackageByFilenameVersionStmt)
	}
}

func (db *SqliteDb) walk(version string, walker func(path, pkg string) bool) {
	rows := db.getPackages.Query(version)
	defer rows.Close()

	for rows.Next() {
		var path string
		var pkg string

		err := rows.Scan(&path, &pkg)
		if err != nil {
			panic(err)
		}

		ret := walker(path, pkg)
		if !ret {
			return
		}
	}
}

func (db *SqliteDb) insertPackageFile(version, path, filePackage string) {
	db.insertPackageFileStmt.Exec(version, path, filePackage)
}

func (db *SqliteDb) beginTransaction() {
	if db.inTransaction {
		return
	}

	_, err := db.db.Exec("BEGIN TRANSACTION")
	if err != nil {
		panic(err)
	}

	db.inTransaction = true
}

func (db *SqliteDb) endTransaction() {
	if !db.inTransaction {
		return
	}

	db.db.Exec("END TRANSACTION")

	db.inTransaction = false
}

func (db *SqliteDb) setETag(version, etag string) {
	db.setETagStmt.Exec(version, etag)
}

func (db *SqliteDb) getETag(version string) string {
	var etag string

	rows := db.getETagStmt.Query(version)
	defer rows.Close()

	if !rows.Next() {
		return ""
	}

	err := rows.Scan(&etag)
	if err != nil {
		panic(err)
	}

	return etag
}
