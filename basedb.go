package godebian

import (
	"database/sql"
	"log"
	"sync"
)

type baseDB struct {
	db *sql.DB
	sync.Mutex
}

type stmt struct {
	name    string
	stmtStr string
	stmt    *sql.Stmt
	db      *baseDB
}

func (s *stmt) Query(args ...interface{}) *sql.Rows {
	s.db.Lock()
	rows, err := s.stmt.Query(args...)
	s.db.Unlock()
	if err != nil {
		log.Fatalf("%s for arguments '%+v' failed: %v", s.name, args, err)
	}

	return rows
}

func (s *stmt) Exec(args ...interface{}) sql.Result {
	s.db.Lock()
	result, err := s.stmt.Exec(args...)
	s.db.Unlock()
	if err != nil {
		log.Fatalf("%s for arguments '%+v' failed: %v", s.name, args, err)
	}

	return result
}

func (db *baseDB) newStmt(name, stmtStr string) *stmt {
	var err error
	stmt := &stmt{}

	stmt.name = name
	stmt.stmtStr = stmtStr
	stmt.db = db

	stmt.stmt, err = db.db.Prepare(stmtStr)
	if err != nil {
		log.Fatal(name + ": could not prepare statement: " + err.Error())
	}

	return stmt
}
