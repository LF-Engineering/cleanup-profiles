package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	gSQLOut = false
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
	gSQLOut = os.Getenv("SQLDEBUG") != ""
	return d
}

// queryOut - display DB query
func queryOut(query string, args ...interface{}) {
	log.Println(query)
	str := ""
	if len(args) > 0 {
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				str += fmt.Sprintf("%d:%+v ", vi+1, v)
			case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *complex64, *complex128, *string, *bool, *time.Time:
				str += fmt.Sprintf("%d:%+v ", vi+1, v)
			case nil:
				str += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				str += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv))
			}
		}
		log.Println("[" + str + "]")
	}
	fmt.Printf("%s\n", query)
	if str != "" {
		fmt.Printf("[%s]\n", str)
	}
}

// queryDB - query database without transaction
func queryDB(db *sqlx.DB, query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = db.Query(query, args...)
	if err != nil || gSQLOut {
		if err != nil {
			log.Println("queryDB failed")
		}
		queryOut(query, args...)
	}
	return
}

// queryTX - query database with transaction
func queryTX(db *sql.Tx, query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = db.Query(query, args...)
	if err != nil || gSQLOut {
		if err != nil {
			log.Println("queryTX failed")
		}
		queryOut(query, args...)
	}
	return
}

// query - query DB using transaction if provided
func query(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (*sql.Rows, error) {
	if tx == nil {
		return queryDB(db, query, args...)
	}
	return queryTX(tx, query, args...)
}

// execDB - execute DB query without transaction
func execDB(db *sqlx.DB, query string, args ...interface{}) (res sql.Result, err error) {
	res, err = db.Exec(query, args...)
	if err != nil || gSQLOut {
		if err != nil {
			log.Println("execDB failed")
		}
		queryOut(query, args...)
	}
	return
}

// execTX - execute DB query with transaction
func execTX(db *sql.Tx, query string, args ...interface{}) (res sql.Result, err error) {
	res, err = db.Exec(query, args...)
	if err != nil || gSQLOut {
		if err != nil {
			log.Println("execTX failed")
		}
		queryOut(query, args...)
	}
	return
}

// exec - execute db query with transaction if provided
func exec(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	if tx == nil {
		return execDB(db, query, args...)
	}
	return execTX(tx, query, args...)
}

func getThreadsNum() int {
	nCPUsStr := os.Getenv("N_CPUS")
	nCPUs := 0
	if nCPUsStr != "" {
		var err error
		nCPUs, err = strconv.Atoi(nCPUsStr)
		if err != nil || nCPUs < 0 {
			nCPUs = 0
		}
	}
	if nCPUs > 0 {
		n := runtime.NumCPU()
		if nCPUs > n {
			nCPUs = n
		}
		runtime.GOMAXPROCS(nCPUs)
		return nCPUs
	}
	thrN := runtime.NumCPU()
	runtime.GOMAXPROCS(thrN)
	return thrN
}

func cleanupProfiles(db *sqlx.DB) (err error) {
	var (
		rows      *sql.Rows
		ids       []string
		uuids     []*string
		sources   []string
		names     []*string
		usernames []*string
		emails    []*string
		id        string
		uuid      *string
		source    string
		name      *string
		username  *string
		email     *string
	)
	processIdentity := func(ch chan error, i int) (err error) {
		defer func() {
			if ch != nil {
				ch <- err
			}
		}()
		fmt.Printf("processing %d\n", i)
		return
	}
	rows, err = query(db, nil, "select id, uuid, source, name, username, email from identities where name like '%%-MISSING-NAME'")
	if err != nil {
		return
	}
	thrN := getThreadsNum()
	fmt.Printf("using %d threads\n", thrN)
	for rows.Next() {
		err = rows.Scan(&id, &uuid, &source, &name, &username, &email)
		if err != nil {
			return
		}
		// log.Println(id, uuid, source, name, username, email)
		ids = append(ids, id)
		uuids = append(uuids, uuid)
		sources = append(sources, source)
		names = append(names, name)
		usernames = append(usernames, username)
		emails = append(emails, email)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	errs := []error{}
	if thrN > 0 {
		ch := make(chan error)
		nThreads := 0
		for i := range ids {
			go func(ch chan error, i int) {
				_ = processIdentity(ch, i)
			}(ch, i)
			nThreads++
			if nThreads == thrN {
				e := <-ch
				nThreads--
				if e != nil {
					errs = append(errs, e)
				}
			}
		}
		for nThreads > 0 {
			e := <-ch
			nThreads--
			if e != nil {
				errs = append(errs, e)
			}
		}
	} else {
		for i := range ids {
			e := processIdentity(nil, i)
			if e != nil {
				errs = append(errs, e)
			}
		}
	}
	nErrs := len(errs)
	if nErrs > 0 {
		err = fmt.Errorf("%d errors: %+v", nErrs, errs)
	}
	return
}

func main() {
	db := initAffsDB()
	err := cleanupProfiles(db)
	if err != nil {
		fmt.Printf("error: %+v\n", err)
	}
}
