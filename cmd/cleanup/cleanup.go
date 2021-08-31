package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func initAffsDB() *sqlx.DB {
	dbURL := os.Getenv("DB_ENDPOINT")
	if !strings.Contains(dbURL, "parseTime=true") {
		if strings.Contains(dbURL, "?") {
			dbURL += "&parseTime=true"
		} else {
			dbURL += "?parseTime=true"
		}
	}
	d, err := sqlx.Connect("mysql", dbURL)
	if err != nil {
		log.Panicf("unable to connect to affiliation database: %v", err)
	}
	return d
}

func main() {
	db := initAffsDB()
	fmt.Printf("%+v\n", db)
}
