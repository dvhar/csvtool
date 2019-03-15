package main
import (
    _ "github.com/denisenkom/go-mssqldb"
    //"github.com/Jeffail/gabs"
    "database/sql"
    "net/url"
    "strings"
    "errors"
    . "fmt"
    "flag"
    "os"
)

//command line flags
var dbNoCon = flag.Bool("no", true, "Don't connect to database")
var localPort = flag.String("port", "8060", "Change localhost port")
var danger = flag.Bool("danger",false, "Allow connections from non-localhost. Dangerous, only use for debugging.")
var dbserver = flag.String("s", os.Getenv("MSSQL_CLI_SERVER"), "Database URL")
var dbname = flag.String("d", os.Getenv("MSSQL_CLI_DATABASE"), "Database name")
var dblogin = flag.String("u", os.Getenv("MSSQL_CLI_USER"), "Database login user")
var dbpass = flag.String("p", "", "Database login password")


//one SingleQueryResult struct holds the results of one query
type SingleQueryResult struct {
    Numrows int
    Numcols int
    Types []int
    Colnames []string
    Vals [][]interface{}
    Status int
    Query string
}

//channel data
const (
    CH_HEADER = iota
    CH_ROW = iota
    CH_DONE = iota
    CH_SAVPREP = iota
)
type chanData struct {
    Message string
    Number int
    Type int
    Header []string
    Row *[]interface{}
}

//query return data struct and codes
const (
    DAT_ERROR = 1 << iota
    DAT_GOOD = 1 << iota
    DAT_BADPATH = 1 << iota
    DAT_IOERR = 1 << iota
    DAT_BLANK = 0
)
type ReturnData struct {
    Entries []*SingleQueryResult
    Status int
    Message string
    OriginalQuery string
    Mode string
    Clipped bool
}

//database connection struct and codes
const (
    CON_ERROR = 1 << iota
    CON_CHANGED = 1 << iota
    CON_UNCHANGED = 1 << iota
    CON_CHECKED = 1 << iota
    CON_BLANK = 0
)
type Connection struct {
    Db *sql.DB
    Err error
    Status int
    Login string
    Server string
    Database string
}

//file io struct and codes
const (
    FP_SERROR = 1 << iota
    FP_SCHANGED = 1 << iota
    FP_OERROR = 1 << iota
    FP_OCHANGED = 1 << iota
    FP_CWD = 0
    F_CSV = 1 << iota
    F_JSON = 1 << iota
    F_OPEN = 1 << iota
    F_SAVE = 1 << iota
)
type FilePaths struct {
    SavePath string
    RtSavePath string
    OpenPath string
    CsvPath string
    Status int
}
//struct that matches incoming json requests
type Qrequest struct {
    Query string
    Qamount int
    Mode string
    FileIO int
    FilePath string
    CsvFile string
}

var dbCon Connection
var FPaths FilePaths
var messager chan string
var saver chan chanData

func main() {
    //get password and other flags
    flag.Parse()
    messager = make(chan string)
    saver = make(chan chanData)
    go realtimeCsvSaver()
    if *dbpass == "" { *dbpass = os.Getenv("MSSQL_CLI_PASSWORD") }

    //initialize file paths
    cwd, err := os.Getwd()
    if err == nil {
        FPaths.OpenPath = cwd + "/"
        FPaths.SavePath  = cwd + "/"
        FPaths.Status = 0
    } else {
        FPaths.Status = FP_OERROR | FP_SERROR
    }

    //set up server url and start server in goroutine
    println("Starting server")
    host := "localhost"
    port := ":" + *localPort
    if *danger { host = "" }
    serverUrl := host + port

    //launch web browser for gui
    launch("http://localhost"+port);

    //if connecting to database
    if (! *dbNoCon) {
        println("attempting database connection")
        dbCon = sqlConnect(*dblogin, *dbpass, *dbserver, *dbname)
        defer dbCon.Db.Close()
    }
    httpserver(serverUrl)

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
        db,_ := sql.Open("mssql", connectString)
        err := db.Ping()

        //prepare return struct
        ret = Connection{ Db: db, Err: err}
        if err != nil {
            println("db connection error")
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

//wrapper for csvQuery
func runCsvQuery(query string, req *Qrequest) (*SingleQueryResult,error) {
    qSpec := QuerySpecs{
        Qstring : query,
        Fname : "/home/dave/Documents/work/data/bigdata/2018facilityclaims.csv" }
    if (req.FileIO & F_SAVE) != 0 { qSpec.Save = true }
    println("attempting csv query from gui program")
    res, err := csvQuery(qSpec)
    res.Query = query;
    Println("csv query error: ",err)
    return res, err
}

//return SingleQueryResult struct with query results
func runSqlServerQuery(db *sql.DB, query string) (*SingleQueryResult,error) {

    //if connected to SQL server
    if (dbCon.Status & CON_CHANGED != 0) {

        rows,err := db.Query(query)
        if err == nil {
            columnNames,_ := rows.Columns()
            columnValues := make([]string, len(columnNames))
            columnPointers := make([]interface{}, len(columnNames))
            for i,_ := range columnNames {
                columnPointers[i] = &columnValues[i]
            }
            var entry []interface{}
            var entries[][]interface{}
            var rownum = 0
            for rows.Next() {
                rows.Scan(columnPointers...)
                entry = make([]interface{},len(columnNames))
                for i,_ := range columnNames {
                    entry[i] = columnValues[i]
                }
                entries = append(entries,entry)
                rownum++
            }
            println("query success")
            return &SingleQueryResult{Colnames: columnNames, Numcols: len(columnNames), Numrows: rownum, Vals: entries, Query: query}, nil
        } else {
            println("query failed")
            return &SingleQueryResult{}, err
        }
    } else {
        println("query null because db not connected")
        return &SingleQueryResult{}, errors.New("no connection")
    }
}

//run Qrequest with multiple queries deliniated by semicolon
func runQueries(db *sql.DB, req *Qrequest) ([]*SingleQueryResult, error) {
    query := req.Query
    if (strings.HasSuffix(query,";")) { query = query[:len(query)-1] }
    queries := strings.Split(strings.Replace(query,"\\n","",-1),";")
    req.Qamount = len(queries)
    //send info to realtime saver
    if (req.FileIO & F_SAVE) != 0 {
        saver <- chanData{
            Number : req.Qamount,
            Type : CH_SAVPREP,
            Message : req.FilePath,
        }
    }
    //run queries in a loop
    var results[]*SingleQueryResult
    var result*SingleQueryResult
    var err error
    for i := range queries {
        //run query
        switch req.Mode {
            case "MSSQL":
                result, err = runSqlServerQuery(db, queries[i])
            case "CSV": fallthrough
            default:
                result, err = runCsvQuery(queries[i], req)
        }

        results = append(results, result)
        if err != nil {
            return results, err
        }
    }
    return results, nil
}
