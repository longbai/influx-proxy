package hbs

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func InitDb(addr string, maxConn int) {
	var err error
	db, err = sql.Open("mysql", addr)
	if err != nil {
		log.Fatalln(err)
	}
	db.SetMaxIdleConns(maxConn)
	err = db.Ping()
	if err != nil {
		log.Fatalln(err)
	}
}
