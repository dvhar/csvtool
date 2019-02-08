package main
import (
    _ "github.com/denisenkom/go-mssqldb"
    "github.com/GeertJohan/go.rice"
    //"github.com/Jeffail/gabs"
    "encoding/json"
    "encoding/base64"
    "database/sql"
    "crypto/sha1"
    "io/ioutil"
    "net/http"
    "net/url"
    "runtime"
    "strings"
    "errors"
    . "fmt"
    "flag"
    "os/exec"
    "os"
)

//command line flags
var dbNoCon = flag.Bool("no", false, "Don't connect to database")
var localPort = flag.String("port", "8060", "Change localhost port")
var danger = flag.Bool("danger",false, "Allow connections from non-localhost. Dangerous, only use for debugging.")
var dbserver = flag.String("s", os.Getenv("MSSQL_CLI_SERVER"), "Database URL")
var dbname = flag.String("d", os.Getenv("MSSQL_CLI_DATABASE"), "Database name")
var dblogin = flag.String("u", os.Getenv("MSSQL_CLI_USER"), "Database login user")
var dbpass = flag.String("p", "", "Database login password")

const (
    CON_BLANK = 1 << iota
    CON_ERROR = 1 << iota
    CON_CHANGED = 1 << iota
    CON_UNCHANGED = 1 << iota
    CON_CHECKED = 1 << iota
)

//one Qrows struct holds the results of one query
type Qrows struct {
    Numrows int
    Numcols int
    Colnames []string
    Vals [][]interface{}
    Status int
    Query string
}
//status 0 is good
type ReturnData struct {
    Entries []*Qrows
    Status int
    Message string
}

//status 0 is blank
type Connection struct {
    Db *sql.DB
    Err error
    Status int
    Login string
    Server string
    Database string
}

//TODO: find out if program will run on multiple databases. Will need cache for each db
var Qcache map[string]*Qrows
var dbCon Connection

func main() {
    //get password and other flags
    flag.Parse()
    if *dbpass == "" { *dbpass = os.Getenv("MSSQL_CLI_PASSWORD") }

    //initialize query data cache
    Qcache = make(map[string]*Qrows)


    //if connecting to database
    if (! *dbNoCon) {
        dbCon = sqlConnect(*dblogin, *dbpass, *dbserver, *dbname)
    }

    println("Starting server")
    //set up server url and goroutine channel
    host := "localhost"
    port := ":" + *localPort
    if *danger { host = "" }
    serverUrl := host + port
    done := make(chan bool)

    //start server and browser
    go server(serverUrl,done)
    launch("localhost"+port);
    <-done


    //close database connection if there is one
    if (dbCon.Status & CON_BLANK != 0) {
        println("closing connection")
        dbCon.Db.Close()
    }
}

//webserver
func server(serverUrl string, done chan bool) {
    http.Handle("/", http.FileServer(rice.MustFindBox("webgui/build").HTTPBox()))
    http.HandleFunc("/query", queryHandler())
    http.HandleFunc("/query/", queryHandler())
    http.HandleFunc("/login", loginHandler())
    http.HandleFunc("/login/", loginHandler())

    http.ListenAndServe(serverUrl, nil)
    done <- true
}

//returns handler function for query requests from the webgui
func queryHandler() (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {

        //struct that matches incoming json requests
        type Qrequest struct {
            Query string
            Savit bool
        }
        body, _ := ioutil.ReadAll(r.Body)
            println(formatRequest(r))
            println(string(body))
        var req Qrequest
        var entries []*Qrows
        var fullData ReturnData
        var err error
        json.Unmarshal(body,&req)
        fullData.Status = 0

        //return null query if no connection
        if dbCon.Err != nil {
            println("no database connection")
            entries = append(entries,&Qrows{})
            fullData.Message = "No database connection"

        //attempt query if there is a connection
        } else {
            println("requesting query")
            entries,err = runQueries(dbCon.Db, req.Query)
            if err != nil {
                fullData.Status |= 1
                fullData.Message = "Error querying database"
            } else {
                fullData.Message = "Query successful"
            }
        }

        fullData.Entries = entries
        full_json,_ := json.Marshal(fullData)

        //save queries to json file
        if req.Savit {
            println("saving query...")
            file,_ := os.OpenFile("savedQueries.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
            file.WriteString(string(full_json))
            file.Close()
        }
            //Printf("resp: %+v", full_json)
            //println(string(full_json))
        Fprint(w, string(full_json))
    }
}

func loginHandler() (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {

        //struct that matches incoming json requests
        type Lrequest struct {
            Login string
            Pass string
            Server string
            Database string
            Action string
        }
        //struct for return json.
        type Lreturn struct {
            Login string
            Server string
            Database string
            Status int
            Message string
        }
        var ret Lreturn
        ret.Status = dbCon.Status
        ret.Login = dbCon.Login
        ret.Server = dbCon.Server
        ret.Database = dbCon.Database

        //handle request
        body, _ := ioutil.ReadAll(r.Body)
        var req Lrequest
        var full_json []uint8
            println(formatRequest(r))
            println(string(body))
        json.Unmarshal(body,&req)

        switch(req.Action){
            case "login":
                newCon := sqlConnect(req.Login, req.Pass, req.Server, req.Database)
                //prepare response
                if newCon.Err == nil {
                    dbCon = newCon
                    ret.Status = dbCon.Status
                    ret.Login = dbCon.Login
                    ret.Server = dbCon.Server
                    ret.Database = dbCon.Database
                    println("Connected to "+dbCon.Database)
                } else {
                    ret.Status = CON_UNCHANGED
                }

            case "check":
                ret.Status = CON_CHECKED
        }

        switch(dbCon.Status){
            case CON_CHANGED:
                ret.Message = "Logged in to " + dbCon.Database
            case CON_ERROR:
                ret.Message = "Connection Error"
            default:
                ret.Message = "Not logged in"
        }
        full_json,_ = json.Marshal(ret)
        Fprint(w, string(full_json))
    }
}

//initialize database connection
func sqlConnect(login, pass, server, name string) Connection {
    var ret Connection

    //check parameters
    if login == "" || pass == "" || server == "" || name == "" {

        ret.Status = CON_BLANK
        ret.Err = errors.New("No Login Credentials")

    } else {
        //setup and make connection
        port := 1433
        query := url.Values{}
        query.Add("database",name)
        query.Add("connection timeout","30")
        u := &url.URL{
            Scheme:   "sqlserver",
            User:     url.UserPassword(login, pass),
            Host:     Sprintf("%s:%d", server, port),
            RawQuery: query.Encode(),
        }
        connectString := u.String()
        db,err := sql.Open("mssql", connectString)

        //prepare return struct
        ret = Connection{ Db: db, Err: err}
        if err != nil {
            println("connection error")
            ret.Status = CON_ERROR
        } else {
            println("db connection successful")
            ret.Status = CON_CHANGED
            ret.Login = login
            ret.Database = name
            ret.Server = server
        }
    }
    return ret
}

//wrapper for runQuery() that caches results
func runCachingQuery(db *sql.DB, query string) (*Qrows,error) {

    hasher := sha1.New()
    hasher.Write([]byte(query))
    sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
    cachedResult, ok := Qcache[sha]
    if ok {
        println("returning cached result " + sha)
        return cachedResult, nil
    } else {
        println("attempting new query for " + sha)
        result, err := runQuery(db, query)
        if err == nil {
            Qcache[sha] = result
        }
        return result, err
    }
}


//return Qrows struct with query results
func runQuery(db *sql.DB, query string) (*Qrows,error) {
    println(query)

    //if connected to SQL server
    if (dbCon.Status & CON_CHANGED != 0) {

        rows,err := db.Query(query)
        if err == nil {
            columnNames,_ := rows.Columns()
            columnValues := make([]interface{}, len(columnNames))
            columnPointers := make([]interface{}, len(columnNames))
            for i := 0; i < len(columnNames); i++ { columnPointers[i] = &columnValues[i] }
            var entry []interface{}
            var entries[][]interface{}
            var rownum = 0
            for rows.Next() {
                rows.Scan(columnPointers...)
                entry = make([]interface{},len(columnNames))
                for i := 0; i < len(columnNames); i++ {
                    entry[i] = columnValues[i]
                }
                entries = append(entries,entry)
                rownum++
            }
            println("query success")
            return &Qrows{Colnames: columnNames, Numcols: len(columnNames), Numrows: rownum, Vals: entries, Query: query}, nil
        } else {
            println("query failed")
            return &Qrows{}, err
        }
    } else {
        println("query null because db not connected")
        return &Qrows{}, errors.New("no connection")
    }
}

//run multiple queries deliniated by semicolon
func runQueries(db *sql.DB, query string) ([]*Qrows, error) {
    if (strings.HasSuffix(query,";")) { query = query[:len(query)-1] }
    queries := strings.Split(strings.Replace(query,"\\n","",-1),";")
    var results[]*Qrows
    for i := range queries {
        result,err := runCachingQuery(db,queries[i])
        results = append(results, result)
        if err != nil {
            return results, err
        }
    }
    return results, nil
}

//some useful premade queries
func premade(request string ) (string) {
    switch request {
        case "columns_total":
            return "SELECT * FROM information_schema.Columns;"

        case "columns_abridged":
            return `SELECT table_name, column_name, ordinal_position, data_type, is_nullable
                    FROM information_schema.columns;`

        case "columns_withkey":
            return `SELECT c.table_name, c.column_name, c.DATA_TYPE, c.IS_NULLABLE, 
                        k.constraint_type, k.constraint_name
                    FROM information_schema.columns as c 
                    left join
                    (
                        select col.column_name, tab.table_name, tab.constraint_type, tab.constraint_name
                        FROM   information_schema.constraint_column_usage as col
                        join information_schema.table_constraints as tab
                        on col.constraint_name = tab.constraint_name
                        where tab.table_name = col.table_name
                    )
                    as k
                    on c.column_name = k.column_name
                    and c.table_name = k.table_name;`

        case "primaries":
            return `SELECT col.column_name, tab.table_name, tab.constraint_type, col.constraint_name
                    FROM   information_schema.constraint_column_usage as col
                    JOIN information_schema.table_constraints as tab
                    ON col.constraint_name = tab.constraint_name
                    WHERE tab.table_name = col.table_name;`
        default:
            return  ""
    }
}

//show request from browser
func formatRequest(r *http.Request) string {
 var request []string
 url := Sprintf("%v %v %v", r.Method, r.URL, r.Proto)
 request = append(request, url)
 request = append(request, Sprintf("Host: %v", r.Host))
 for name, headers := range r.Header {
   name = strings.ToLower(name)
   for _, h := range headers {
     request = append(request, Sprintf("%v: %v", name, h))
   }
 }
 if r.Method == "POST" {
    r.ParseForm()
    request = append(request, "\n")
    request = append(request, r.Form.Encode())
 }
  return strings.Join(request, "\n")
}

//launch browser
func launch(url string) error {
    var cmd string
    var args []string

    switch runtime.GOOS {
    case "windows":
        cmd = "cmd"
        args = []string{"/c", "start"}
    case "darwin":
        cmd = "open"
    default: // "linux", "freebsd", "openbsd", "netbsd"
        cmd = "xdg-open"
    }
    args = append(args, url)
    return exec.Command(cmd, args...).Start()
}
