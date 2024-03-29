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
	setPackageInfoETagStmt          *stmt
	getPackageInfoETagStmt          *stmt
	insertPackageFileStmt           *stmt
	insertPackageInfoStmt           *stmt
	insertPackagePopularityStmt     *stmt
	getPackageByFilepathVersionStmt *stmt
	getPackageByFilenameVersionStmt *stmt
	getPackagesStmt                 *stmt
	getPopularityByPackageStmt      *stmt
	getPackageInfoStmt              *stmt
	removeAllPackagesStmt           *stmt
	removeAllPackageInfosStmt       *stmt
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

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS etag_contents (version VARCHAR, repo VARCHAR, current VARCHAR, PRIMARY KEY(version, repo))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS etag_popularity (version VARCHAR, current VARCHAR, PRIMARY KEY(version))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS etag_packageinfo (version VARCHAR, repo VARCHAR, arch VARCHAR, current VARCHAR, PRIMARY KEY(version, repo, arch))`)
	if err != nil {
		panic("Could not create table etag: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS file2package (version VARCHAR, repo VARCHAR, path VARCHAR, package VARCHAR, PRIMARY KEY(version, repo, path, package))`)
	if err != nil {
		panic("Could not create table file2package: " + err.Error())
	}
	_, err = db.db.Exec(`CREATE INDEX IF NOT EXISTS file2package_path_idx ON file2package(path);`)
	if err != nil {
		panic("Could not create index on file2package: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS package2popularity (version VARCHAR, package VARCHAR, popularity INTEGER, PRIMARY KEY(version, package))`)
	if err != nil {
		panic("Could not create table package2popularity: " + err.Error())
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS packageinfo (version VARCHAR, repo VARCHAR, package VARCHAR, package_version VARCHAR, arch VARCHAR, filename VARCHAR,
		PRIMARY KEY(version, package, package_version, arch))`)
	if err != nil {
		panic("Could not create table packageinfo: " + err.Error())
	}

	db.prepareStatements()

	_, err = db.db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		panic(err)
	}

	db.inTransaction = false
}

func (db *SqliteDb) prepareStatements() {
	var stmts = []struct {
		name    string
		stmtStr string
		stmt    **stmt
	}{
		{"set contents ETag", "INSERT OR REPLACE INTO etag_contents (version, repo, current) VALUES (?, ?, ?)", &db.setContentETagStmt},
		{"get contents ETag", "SELECT current FROM etag_contents WHERE version = ? AND repo = ?", &db.getContentETagStmt},
		{"set popularity ETag", "INSERT OR REPLACE INTO etag_popularity (version, current) VALUES (?, ?)", &db.setPopularityETagStmt},
		{"get popularity ETag", "SELECT current FROM etag_popularity WHERE version = ?", &db.getPopularityETagStmt},
		{"set packageinfo ETag", "INSERT OR REPLACE INTO etag_packageinfo (version, repo, arch, current) VALUES (?, ?, ?, ?)", &db.setPackageInfoETagStmt},
		{"get packageinfo ETag", "SELECT current FROM etag_packageinfo WHERE version = ? AND repo = ? AND arch = ?", &db.getPackageInfoETagStmt},
		{"insert package file", "INSERT OR REPLACE INTO file2package (version, repo, path, package) VALUES (?, ?, ?, ?)", &db.insertPackageFileStmt},
		{"insert package info", "INSERT OR REPLACE INTO packageinfo (version, repo, package, package_version, arch, filename) VALUES (?, ?, ?, ?, ?, ?)", &db.insertPackageInfoStmt},
		{"insert package popularity", "INSERT OR REPLACE INTO package2popularity (version, package, popularity) VALUES (?, ?, ?)", &db.insertPackagePopularityStmt},
		{"get package by version, repo and file path", `SELECT f2p.package FROM file2package AS f2p LEFT JOIN package2popularity AS p2p
								ON f2p.version = p2p.version
									AND f2p.package = p2p.package
								WHERE f2p.version = ?
									AND f2p.path = ?
								ORDER BY p2p.popularity ASC`, &db.getPackageByFilepathVersionStmt},
		{"get package by version and file name", `SELECT f2p.package FROM file2package AS f2p LEFT JOIN package2popularity AS p2p
								ON f2p.version = p2p.version
									AND f2p.package = p2p.package
								WHERE f2p.version = ?
									AND f2p.path LIKE ?
								ORDER BY p2p.popularity ASC`, &db.getPackageByFilenameVersionStmt},
		{"get package popularity", "SELECT popularity FROM package2popularity WHERE version = ? AND package = ?", &db.getPopularityByPackageStmt},
		{"remove all packages of version, repo", "DELETE FROM file2package WHERE version = ? AND repo = ?", &db.removeAllPackagesStmt},
		{"remove all packageinfos of version, repo and arch", "DELETE FROM packageinfo WHERE version = ? AND repo = ? AND arch = ?", &db.removeAllPackageInfosStmt},
		{"remove all popcons of version", "DELETE FROM package2popularity WHERE version = ?", &db.removeAllPopularitiesStmt},
		{"list packages by version and repo", "SELECT path, package FROM file2package WHERE version = ? AND repo = ?", &db.getPackagesStmt},
		{"get package info", "SELECT filename FROM packageinfo WHERE version = ? AND arch = ? AND package = ?", &db.getPackageInfoStmt},
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

func (db *SqliteDb) removeAllPackageInfos(version, repo, arch string) {
	db.removeAllPackageInfosStmt.Exec(version, repo, arch)
}

func (db *SqliteDb) removeAllPackages(version, repo string) {
	db.removeAllPackagesStmt.Exec(version, repo)
}

func (db *SqliteDb) removeAllPopularities(version string) {
	db.removeAllPopularitiesStmt.Exec(version)
}

func (db *SqliteDb) getPackageInfo(version, arch, pkg string) PackageInfo {
	var pi PackageInfo

	rows := db.getPackageInfoStmt.Query(version, arch, pkg)
	defer rows.Close()

	if !rows.Next() {
		return pi
	}

	var filename string
	err := rows.Scan(&filename)
	if err != nil {
		panic(err)
	}

	pi.Version = version
	pi.Name = pkg
	pi.Filename = filename

	return pi
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

func (db *SqliteDb) insertPackageInfo(version, repo string, arch string, pkginfo PackageInfo) {
	db.insertPackageInfoStmt.Exec(version, repo, pkginfo.Name, pkginfo.Version, arch, pkginfo.Filename)
}

func (db *SqliteDb) insertPackageFile(version, repo, path, filePackage string) {
	db.insertPackageFileStmt.Exec(version, repo, path, filePackage)
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

func (db *SqliteDb) setContentETag(version, repo, etag string) {
	db.setContentETagStmt.Exec(version, repo, etag)
}

func (db *SqliteDb) setPackageInfoETag(version, repo, arch, etag string) {
	db.setPackageInfoETagStmt.Exec(version, repo, arch, etag)
}

func (db *SqliteDb) getPackageInfoETag(version, repo, arch string) string {
	var etag string

	rows := db.getPackageInfoETagStmt.Query(version, repo, arch)
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

func (db *SqliteDb) getContentETag(version, repo string) string {
	var etag string

	rows := db.getContentETagStmt.Query(version, repo)
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
