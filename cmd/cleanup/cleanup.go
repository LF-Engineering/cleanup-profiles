package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/auth0"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	dahttp "github.com/LF-Engineering/dev-analytics-libraries/http"
	"github.com/LF-Engineering/dev-analytics-libraries/slack"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
)

var (
	gSQLOut      = false
	gDebug       = false
	gToken       = ""
	gTokenMtx    = &sync.Mutex{}
	gAuth0Client *auth0.ClientProvider
	gTokenEnv    string
	// MT - multithreading?
	MT bool
	// EmailRegex - regexp to match email address
	EmailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
	// EmailReplacer - replacer for some email buggy characters
	EmailReplacer = strings.NewReplacer(" at ", "@", " AT ", "@", " At ", "@", " dot ", ".", " DOT ", ".", " Dot ", ".", "<", "", ">", "")
	// WhiteSpace - one or more whitespace characters
	WhiteSpace        = regexp.MustCompile(`\s+`)
	emailsCache       = map[string]bool{}
	emailsCacheMtx    *sync.RWMutex
	uuidsAffsCache    = map[string]string{}
	uuidsAffsCacheMtx *sync.RWMutex
)

// uuidAffs - generate UUID of string args
// uses internal cache
// downcases arguments, all but first can be empty
func uuidAffs(args ...string) (h string) {
	k := strings.Join(args, ":")
	if MT {
		uuidsAffsCacheMtx.RLock()
	}
	h, ok := uuidsAffsCache[k]
	if MT {
		uuidsAffsCacheMtx.RUnlock()
	}
	if ok {
		return
	}
	defer func() {
		if MT {
			uuidsAffsCacheMtx.Lock()
		}
		uuidsAffsCache[k] = h
		if MT {
			uuidsAffsCacheMtx.Unlock()
		}
	}()
	var err error
	if len(args) != 4 {
		err = fmt.Errorf("GenerateIdentity requires exactly 4 asrguments, got %+v", args)
	} else {
		h, err = uuid.GenerateIdentity(&args[0], &args[1], &args[2], &args[3])
	}
	if err != nil {
		fmt.Printf("uuidAffs error for: %+v\n", args)
		h = ""
	}
	return
}

// isValidDomain - is MX domain valid?
// uses internal cache
func isValidDomain(domain string) (valid bool) {
	l := len(domain)
	if l < 4 && l > 254 {
		return
	}
	if MT {
		emailsCacheMtx.RLock()
	}
	valid, ok := emailsCache[domain]
	if MT {
		emailsCacheMtx.RUnlock()
	}
	if ok {
		// fmt.Printf("domain cache hit: '%s' -> %v\n", domain, valid)
		return
	}
	defer func() {
		if MT {
			emailsCacheMtx.Lock()
		}
		emailsCache[domain] = valid
		if MT {
			emailsCacheMtx.Unlock()
		}
	}()
	mx, err := net.LookupMX(domain)
	if err != nil || len(mx) == 0 {
		return
	}
	valid = true
	return
}

// isValidEmail - is email correct: len, regexp, MX domain
// uses internal cache
func isValidEmail(email string, validateDomain bool) (valid bool) {
	l := len(email)
	if l < 6 && l > 254 {
		return
	}
	if MT {
		emailsCacheMtx.RLock()
	}
	valid, ok := emailsCache[email]
	if MT {
		emailsCacheMtx.RUnlock()
	}
	if ok {
		// fmt.Printf("email cache hit: '%s' -> %v\n", email, valid)
		return
	}
	defer func() {
		if MT {
			emailsCacheMtx.Lock()
		}
		emailsCache[email] = valid
		if MT {
			emailsCacheMtx.Unlock()
		}
	}()
	email = WhiteSpace.ReplaceAllString(email, " ")
	email = strings.TrimSpace(EmailReplacer.Replace(email))
	email = strings.Split(email, " ")[0]
	if !EmailRegex.MatchString(email) {
		return
	}
	if validateDomain {
		parts := strings.Split(email, "@")
		if len(parts) <= 1 || !isValidDomain(parts[1]) {
			return
		}
	}
	valid = true
	return
}

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
	gDebug = os.Getenv("DEBUG") != ""
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

func getThreadsNum() (thrN int) {
	defer func() {
		MT = thrN > 1
		if MT {
			emailsCacheMtx = &sync.RWMutex{}
			uuidsAffsCacheMtx = &sync.RWMutex{}
		}
	}()
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
		thrN = nCPUs
		return
	}
	thrN = runtime.NumCPU()
	runtime.GOMAXPROCS(thrN)
	return
}

func initializeAuth0() error {
	var err error
	auth0DataB64 := os.Getenv("AUTH0_DATA")
	if auth0DataB64 == "" {
		return fmt.Errorf("you must specify AUTH0_DATA (so the program can generate an API token) or specify token with JWT_TOKEN")
	}
	var auth0Data []byte
	auth0Data, err = base64.StdEncoding.DecodeString(auth0DataB64)
	if err != nil {
		fmt.Printf("decode base64 error: %+v\n", err)
		return err
	}
	var data map[string]string
	err = jsoniter.Unmarshal([]byte(auth0Data), &data)
	if err != nil {
		fmt.Printf("unmarshal error: %+v\n", err)
		return err
	}
	// Providers
	httpClientProvider := dahttp.NewClientProvider(60 * time.Second)
	esCacheClientProvider, err := elastic.NewClientProvider(
		&elastic.Params{
			URL:      data["es_url"],
			Username: data["es_user"],
			Password: data["es_pass"],
		})
	if err != nil {
		fmt.Printf("ES client provider error: %+v\n", err)
		return err
	}
	slackProvider := slack.New(data["slack_webhook_url"])
	gAuth0Client, err = auth0.NewAuth0Client(
		data["env"],
		data["grant_type"],
		data["client_id"],
		data["client_secret"],
		data["audience"],
		data["url"],
		httpClientProvider,
		esCacheClientProvider,
		&slackProvider,
		"identity-profile-cleanup",
	)
	if err == nil {
		gTokenEnv = data["env"]
	}
	return err
}

func getAPIToken() (string, error) {
	envToken := os.Getenv("JWT_TOKEN")
	if envToken != "" {
		return envToken, nil
	}
	if gTokenEnv == "" {
		err := initializeAuth0()
		if err != nil {
			return "", err
		}
	}
	token, err := gAuth0Client.GetToken()
	if err == nil && token != "" {
		token = "Bearer " + token
	}
	return token, err
}

func executeAffiliationsAPICall(apiPath, path string) (err error) {
	if apiPath == "" {
		err = fmt.Errorf("Cannot execute DA affiliation API calls, no API URL specified")
		return
	}
	gTokenMtx.Lock()
	if gToken == "" {
		gToken = os.Getenv("JWT_TOKEN")
	}
	if gToken == "" {
		fmt.Printf("obtaining API token\n")
		gToken, err = getAPIToken()
		if err != nil {
			gTokenMtx.Unlock()
			fmt.Printf("get API token error: %+v\n", err)
			os.Exit(1)
			return
		}
	}
	gTokenMtx.Unlock()
	method := http.MethodPut
	rurl := path
	url := apiPath + rurl
	for i := 0; i < 2; i++ {
		req, e := http.NewRequest(method, url, nil)
		if e != nil {
			err = fmt.Errorf("new request error: %+v for %s url: %s", e, method, rurl)
			return
		}
		gTokenMtx.Lock()
		req.Header.Set("Authorization", gToken)
		gTokenMtx.Unlock()
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			err = fmt.Errorf("do request error: %+v for %s url: %s", e, method, rurl)
			return
		}
		if i == 0 && resp.StatusCode == 401 {
			_ = resp.Body.Close()
			fmt.Printf("token is invalid, trying to generate another one\n")
			gTokenMtx.Lock()
			gToken, err = getAPIToken()
			gTokenMtx.Unlock()
			if err != nil {
				fmt.Printf("get API token error: %+v\n", err)
				os.Exit(1)
				return
			}
			continue
		}
		if resp.StatusCode != 200 {
			body, e := ioutil.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if e != nil {
				err = fmt.Errorf("readAll non-ok request error: %+v for %s url: %s", e, method, rurl)
				return
			}
			err = fmt.Errorf("method:%s url:%s status:%d\n%s", method, rurl, resp.StatusCode, body)
			return
		}
		break
	}
	return
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
		mtx       *sync.Mutex
	)
	apiPath := os.Getenv("API_URL")
	if apiPath == "" {
		err = fmt.Errorf("API_URL must be set")
		return
	}
	merges := 0
	idMap := map[string]string{}
	uuidMap := map[string]string{}
	getKey := func(source string, username, email *string) (key string) {
		key = source
		if username != nil && *username != "" {
			key += ":" + *username
		}
		if email != nil && *email != "" {
			key += ":" + *email
		}
		return
	}
	processIdentity := func(ch chan error, i int) (err error) {
		defer func() {
			if ch != nil {
				ch <- err
			}
		}()
		source := sources[i]
		username := usernames[i]
		email := emails[i]
		key := getKey(source, username, email)
		uuid2, ok := uuidMap[key]
		if !ok {
			return
		}
		puuid := uuids[i]
		if puuid == nil {
			return
		}
		uuid := *puuid
		if uuid == uuid2 {
			return
		}
		id := ids[i]
		id2, _ := idMap[key]
		if id != uuid {
			fmt.Printf("complex #%d (%s,%s) -> (%s,%s)\n", i, id, uuid, id2, uuid2)
		}
		fmt.Printf("merge #%d %s -> %s\n", i, uuid, uuid2)
		// curl_put_merge_unique_identities.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a "$ar1"
		err = executeAffiliationsAPICall(apiPath, "/v1/affiliation/no-project/merge_unique_identities/"+uuid+"/"+uuid2+"?archive=true")
		if err != nil {
			fmt.Printf("merge error: %+v\n", err)
			return
		}
		fmt.Printf("merged #%d %s -> %s\n", i, uuid, uuid2)
		if mtx != nil {
			mtx.Lock()
		}
		merges++
		if mtx != nil {
			mtx.Unlock()
		}
		return
	}
	rows, err = query(
		db,
		nil,
		"select id, uuid, source, name, username, email from identities where name like '%%-MISSING-NAME' "+
			"and ((username is not null and trim(username) != '') or (email is not null and trim(email) != ''))",
	)
	if err != nil {
		return
	}
	var missingMap map[string]struct{}
	if gDebug {
		missingMap = make(map[string]struct{})
	}
	thrN := getThreadsNum()
	fmt.Printf("Using %d threads\n", thrN)
	for rows.Next() {
		err = rows.Scan(&id, &uuid, &source, &name, &username, &email)
		if err != nil {
			return
		}
		if gDebug {
			key := getKey(source, username, email)
			_, dup := missingMap[key]
			if dup {
				fmt.Printf("missing names: non-unique key: %s\n", key)
			}
			missingMap[key] = struct{}{}
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
	fmt.Printf("%d identities with missing name suffix and non-empty username or email\n", len(ids))
	rows, err = query(
		db,
		nil,
		"select id, uuid, source, username, email from identities where (name is null or trim(name) = '') "+
			"and ((username is not null and trim(username) != '') or (email is not null and trim(email) != ''))",
	)
	if err != nil {
		return
	}
	emptyMap := map[string]struct{}{}
	for rows.Next() {
		err = rows.Scan(&id, &uuid, &source, &username, &email)
		if err != nil {
			return
		}
		key := getKey(source, username, email)
		_, dup := emptyMap[key]
		if dup {
			fmt.Printf("empty names: non-unique key: %s\n", key)
			// We merge into first found, to skip such cases uncomment 'continue' line
			// continue
		}
		emptyMap[key] = struct{}{}
		idMap[key] = id
		if uuid != nil {
			uuidMap[key] = *uuid
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	fmt.Printf("%d identities with empty/null name and non-empty username or email\n", len(emptyMap))
	errs := []error{}
	if thrN > 0 {
		mtx = &sync.Mutex{}
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
	if merges > 0 {
		fmt.Printf("merged %d profiles\n", merges)
	}
	if os.Getenv("DELETE_ORPHANED") != "" {
		res, e := exec(db, nil, "delete from uidentities where uuid not in (select uuid from identities)")
		if e != nil {
			errs = append(errs, e)
		} else {
			affected, _ := res.RowsAffected()
			if affected > 0 {
				fmt.Printf("deleted %d orphaned profiles\n", affected)
			}
		}
	}
	nErrs := len(errs)
	if nErrs > 0 {
		err = fmt.Errorf("%d errors: %+v", nErrs, errs)
	}
	return
}

func cleanupEmails(db *sqlx.DB) (err error) {
	thrN := getThreadsNum()
	fmt.Printf("Using %d threads\n", thrN)
	var (
		id        string
		source    string
		name      string
		username  string
		email     string
		ids       []string
		sources   []string
		names     []string
		usernames []string
		emails    []string
		rows      *sql.Rows
		mtx       *sync.Mutex
	)
	rows, err = query(db, nil, "select id, source, coalesce(name, ''), coalesce(username, ''), email from identities where email is not null and trim(email) != ''")
	if err != nil {
		return
	}
	for rows.Next() {
		err = rows.Scan(&id, &source, &name, &username, &email)
		if err != nil {
			return
		}
		ids = append(ids, id)
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
	n := len(ids)
	fmt.Printf("%d identities with non-empty email\n", n)
	validateDomain := os.Getenv("SKIP_VALIDATE_DOMAIN") == ""
	updates, mismatch := 0, 0
	errs := []error{}
	processIdentity := func(ch chan error, i int) (err error) {
		defer func() {
			if ch != nil {
				ch <- err
			}
		}()
		email := emails[i]
		valid := isValidEmail(email, validateDomain)
		if valid {
			return
		}
		if gDebug {
			fmt.Printf("processing identity invalid email #%d: '%s'\n", i, email)
		}
		id := ids[i]
		source := sources[i]
		name := names[i]
		username := usernames[i]
		prevUUID := uuidAffs(source, email, name, username)
		if gDebug && prevUUID != id {
			fmt.Printf("notice: old identity ID calculation mismatch for (src=%s,email=%s,name=%s,uname=%s)\n", source, email, name, username)
		}
		uuid := uuidAffs(source, "", name, username)
		var res sql.Result
		res, err = exec(db, nil, "update identities set email = '', id = ? where id = ?", uuid, id)
		if err != nil {
			fmt.Printf("correct identity with an empty email already exists (src=%s,name=%s,uname=%s), deleting current %s\n", source, name, username, id)
			if strings.Contains(err.Error(), "Duplicate entry") {
				res, err = exec(db, nil, "delete from identities where id = ?", id)
				if err != nil {
					return
				}
			} else {
				return
			}
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			fmt.Printf("no rows affected for (%s->%s,src=%s,email=%s,name=%s,uname=%s)\n", id, uuid, source, email, name, username)
			return
		}
		if gDebug {
			fmt.Printf("affected = %d for (%s->%s,src=%s,email=%s,name=%s,uname=%s)\n", affected, id, uuid, source, email, name, username)
		}
		if mtx != nil {
			mtx.Lock()
		}
		updates++
		if prevUUID != id {
			mismatch++
		}
		if mtx != nil {
			mtx.Unlock()
		}
		return
	}
	if thrN > 0 {
		mtx = &sync.Mutex{}
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
	if updates > 0 {
		fmt.Printf("updated %d identities, UUID mismatch: %d\n", updates, mismatch)
	}
	// Profiles
	rows, err = query(db, nil, "select uuid, email from profiles where email is not null and trim(email) != ''")
	if err != nil {
		return
	}
	var (
		puuid   string
		pemail  string
		puuids  []string
		pemails []string
	)
	for rows.Next() {
		err = rows.Scan(&puuid, &pemail)
		if err != nil {
			return
		}
		puuids = append(puuids, puuid)
		pemails = append(pemails, pemail)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	np := len(puuids)
	fmt.Printf("%d profiles with non-empty email\n", np)
	pupdates := 0
	processProfile := func(ch chan error, i int) (err error) {
		defer func() {
			if ch != nil {
				ch <- err
			}
		}()
		email := pemails[i]
		valid := isValidEmail(email, validateDomain)
		if valid {
			return
		}
		if gDebug {
			fmt.Printf("processing profile invalid email #%d: '%s'\n", i, email)
		}
		uuid := puuids[i]
		var res sql.Result
		res, err = exec(db, nil, "update profiles set email = '' where uuid = ?", uuid)
		if err != nil {
			return
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			fmt.Printf("no rows affected for (uuid=%s,email=%s)\n", uuid, email)
			return
		}
		if gDebug {
			fmt.Printf("affected = %d for (%s,%s)\n", affected, uuid, email)
		}
		if mtx != nil {
			mtx.Lock()
		}
		pupdates++
		if mtx != nil {
			mtx.Unlock()
		}
		return
	}
	if thrN > 0 {
		mtx = &sync.Mutex{}
		ch := make(chan error)
		nThreads := 0
		for i := range puuids {
			go func(ch chan error, i int) {
				_ = processProfile(ch, i)
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
		for i := range puuids {
			e := processProfile(nil, i)
			if e != nil {
				errs = append(errs, e)
			}
		}
	}
	if pupdates > 0 {
		fmt.Printf("updated %d profiles\n", pupdates)
	}
	return
}

func main() {
	db := initAffsDB()
	op := os.Getenv("CLEANUP_PROFILES") != ""
	if op {
		err := cleanupProfiles(db)
		if err != nil {
			fmt.Printf("cleanup profiles error: %+v\n", err)
		}
	}
	op = os.Getenv("CLEANUP_EMAILS") != ""
	if op {
		err := cleanupEmails(db)
		if err != nil {
			fmt.Printf("cleanup emails error: %+v\n", err)
		}
		if gDebug {
			fmt.Printf("email cache:\n%+v\n", emailsCache)
		}
	}
}
