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

	setContentETagStmt              *stmt
	getContentETagStmt              *stmt
	setPopularityETagStmt           *stmt
	getPopularityETagStmt           *stmt
	insertPackageFileStmt           *stmt
	insertPackagePopularityStmt     *stmt
	getPackageByFilepathVersionStmt *stmt
	getPackageByFilenameVersionStmt *stmt
	getPackagesStmt                 *stmt
	getPopularityByPackageStmt      *stmt
	getPopularityByPackage          *stmt
	removeAllPackagesStmt           *stmt
	removeAllPopularitiesStmt       *stmt
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

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS etag_contents (version VARCHAR, current VARCHAR, PRIMARY KEY(version))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS etag_popularity (version VARCHAR, current VARCHAR, PRIMARY KEY(version))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS file2package (version VARCHAR, path VARCHAR, package VARCHAR, PRIMARY KEY(version, path, package))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS package2popularity (version VARCHAR, package VARCHAR, popularity INTEGER, PRIMARY KEY(version, package))`)
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
		{"set contents ETag", "INSERT OR REPLACE INTO etag_contents (version, current) VALUES (?, ?)", &db.setContentETagStmt},
		{"get contents ETag", "SELECT current FROM etag_contents WHERE version = ?", &db.getContentETagStmt},
		{"set popularity ETag", "INSERT OR REPLACE INTO etag_popularity (version, current) VALUES (?, ?)", &db.setPopularityETagStmt},
		{"get popularity ETag", "SELECT current FROM etag_popularity WHERE version = ?", &db.getPopularityETagStmt},
		{"insert package file", "INSERT OR REPLACE INTO file2package (version, path, package) VALUES (?, ?, ?)", &db.insertPackageFileStmt},
		{"insert package popularity", "INSERT OR REPLACE INTO package2popularity (version, package, popularity) VALUES (?, ?, ?)", &db.insertPackagePopularityStmt},
		{"get package by version and file path", "SELECT package FROM file2package WHERE version = ? AND path = ?", &db.getPackageByFilepathVersionStmt},
		{"get package by version and file name", "SELECT package FROM file2package WHERE version = ? AND path LIKE ?", &db.getPackageByFilenameVersionStmt},
		//{"get most popular package by version and file name", "SELECT package FROM file2package AS WHERE version = ? AND path LIKE ?", &db.getPackageByFilenameVersionStmt},
		{"get package popularity", "SELECT popularity FROM package2popularity WHERE version = ? AND package = ?", &db.getPopularityByPackageStmt},
		{"remove all packages of version", "DELETE FROM file2package WHERE version = ?", &db.removeAllPackagesStmt},
		{"remove all popcons of version", "DELETE FROM package2popularity WHERE version = ?", &db.removeAllPopularitiesStmt},
		{"list packages by version", "SELECT path, package FROM file2package WHERE version = ?", &db.getPackagesStmt},
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

func (db *SqliteDb) removeAllPopularities(version string) {
	db.removeAllPopularitiesStmt.Exec(version)
}

func (db *SqliteDb) getPackagePopularity(version, pkg string) uint {
	rows := db.getPopularityByPackageStmt.Query(version, pkg)
	defer rows.Close()

	if !rows.Next() {
		return 0
	}

	var popularity uint
	err := rows.Scan(&popularity)
	if err != nil {
		panic(err)
	}

	return popularity
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
	rows := db.getPackagesStmt.Query(version)
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

func (db *SqliteDb) insertPackagePopularity(version, pkg string, popularity uint) {
	db.insertPackagePopularityStmt.Exec(version, pkg, popularity)
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

func (db *SqliteDb) setContentETag(version, etag string) {
	db.setContentETagStmt.Exec(version, etag)
}

func (db *SqliteDb) getContentETag(version string) string {
	var etag string

	rows := db.getContentETagStmt.Query(version)
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

func (db *SqliteDb) setPopularityETag(version, etag string) {
	db.setPopularityETagStmt.Exec(version, etag)
}

func (db *SqliteDb) getPopularityETag(version string) string {
	var etag string

	rows := db.getPopularityETagStmt.Query(version)
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
