package main
import (
    _ "github.com/denisenkom/go-mssqldb"
    "github.com/GeertJohan/go.rice"
    //"github.com/Jeffail/gabs"
    "encoding/base64"
    "encoding/json"
    "encoding/csv"
    "path/filepath"
    "database/sql"
    "crypto/sha1"
    "io/ioutil"
    "net/http"
    "net/url"
    "runtime"
    "strings"
    "errors"
    . "fmt"
    "time"
    "flag"
    "os/exec"
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
    OpenPath string
    CsvPath string
    Status int
}
//struct that matches incoming json requests
type Qrequest struct {
    Query string
    Mode string
    Cache bool
    FileIO int
    FilePath string
    CsvFile string
}

//TODO: find out if program will run on multiple databases. Will need cache for each db
var Qcache map[string]*SingleQueryResult
var dbCon Connection
var FPaths FilePaths

func main() {
    //get password and other flags
    flag.Parse()
    if *dbpass == "" { *dbpass = os.Getenv("MSSQL_CLI_PASSWORD") }

    //initialize query data cache and file paths
    Qcache = make(map[string]*SingleQueryResult)
    cwd, err := os.Getwd()
    if err == nil {
        FPaths.OpenPath = cwd + "/"
        FPaths.SavePath  = cwd + "/sqlSaved.json"
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
    //done := make(chan bool)
    //go server(serverUrl,done)

    //launch web browser for gui
    launch("http://localhost"+port);

    //if connecting to database
    if (! *dbNoCon) {
        println("attempting database connection")
        dbCon = sqlConnect(*dblogin, *dbpass, *dbserver, *dbname)
        defer dbCon.Db.Close()
    }
    server(serverUrl)
    //<-done

}

//webserver
//func server(serverUrl string, done chan bool) {
func server(serverUrl string) {
    http.Handle("/", http.FileServer(rice.MustFindBox("webgui/build").HTTPBox()))
    http.HandleFunc("/query", queryHandler())
    http.HandleFunc("/query/", queryHandler())
    http.HandleFunc("/login", loginHandler())
    http.HandleFunc("/login/", loginHandler())
    http.HandleFunc("/info", infoHandler())
    http.HandleFunc("/info/", infoHandler())
    http.ListenAndServe(serverUrl, nil)
    //done <- true
}

//returns handler function for query requests from the webgui
func queryHandler() (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {

        body, _ := ioutil.ReadAll(r.Body)
            println(formatRequest(r))
            println(string(body))
        var req Qrequest
        var entries []*SingleQueryResult
        var fullReturnData ReturnData
        var err error
        json.Unmarshal(body,&req)
        fullReturnData.Status = DAT_BLANK
        fullReturnData.OriginalQuery = req.Query
        fullReturnData.Mode = req.Mode

        //handle request to open file
        if (req.FileIO & F_OPEN) != 0 {
            full_json,_ := openQueryFile(&req, &fullReturnData)
            Fprint(w, string(full_json))
            return
        }

        //return null query if no connection
        if req.Mode == "MSSQL" && dbCon.Err != nil {
            println("no database connection")
            entries = append(entries,&SingleQueryResult{})
            fullReturnData.Message = "No database connection"

        //attempt query
        } else {
            println("requesting query")
            entries,err = runQueries(dbCon.Db, &req)
            if err != nil {
                fullReturnData.Status |= DAT_ERROR
                fullReturnData.Message = "Error querying database"
            } else {
                fullReturnData.Status |= DAT_GOOD
                fullReturnData.Message = "Query successful"
            }
        }

        fullReturnData.Entries = entries
        full_json,_ := json.Marshal(fullReturnData)

        //save queries to file
        if (req.FileIO & F_SAVE) != 0 {
            saveQueryFile(&req, &fullReturnData, &full_json)
        }

        //update json with save message
        rowLimit(&fullReturnData)
        full_json,_ = json.Marshal(fullReturnData)
        Fprint(w, string(full_json))
        full_json = []byte("")
        fullReturnData = ReturnData{}
        println("running garbage collector")
        runtime.GC()
    }
}

//limit the amount of rows returned to the browser because browsers are slow
func rowLimit(fullReturnData *ReturnData) {
    for i, query := range fullReturnData.Entries {
        if query.Numrows > 1000 {
            fullReturnData.Entries[i].Vals = query.Vals[:1000]
            fullReturnData.Entries[i].Numrows = 1000
            runtime.GC()
        }
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
        //struct for return json
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
        //println(formatRequest(r))
        println("got login request")
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

func infoHandler() (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        //request and response types
        type Irequest struct {
            Info string
        }
        type Ireturn struct {
            SavePath string
            OpenPath string
            Status int
        }
        body, _ := ioutil.ReadAll(r.Body)
        var req Irequest
        var ret Ireturn
        json.Unmarshal(body,&req)

        //currently only returns paths
        ret.SavePath = FPaths.SavePath
        ret.OpenPath = FPaths.OpenPath
        ret.Status = FPaths.Status
        println("s: "+ ret.SavePath + "\no: " +ret.OpenPath)

        full_json,_ := json.Marshal(ret)
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

//wrapper for runSqlServerQuery() that caches results
func runCachingQuery(db *sql.DB, query string, mode string, cacheOn bool) (*SingleQueryResult, error) {

    hasher := sha1.New()
    hasher.Write([]byte(query))
    sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
    cachedResult, ok := Qcache[sha]
    if ok {
        println("returning cached result " + sha)
        return cachedResult, nil
    } else {
        switch mode {
            case "CSV":
                println("attempting new csv query for " + sha)
                result, err := runCsvQuery(query)
                if err == nil && cacheOn {
                    println("caching result")
                    Qcache[sha] = result
                }
                return result, err
            case "MSSQL": fallthrough
            default:
                println("attempting new server query for " + sha)
                result, err := runSqlServerQuery(db, query)
                if err == nil && cacheOn {
                    println("caching result")
                    Qcache[sha] = result
                }
                return result, err
        }
        println("invalid query request object")
        return nil, errors.New("invalid query request object")
    }
}

//wrapper for csvQuery
func runCsvQuery(query string) (*SingleQueryResult,error) {
    qSpec := QuerySpecs{
        Qstring : query,
        Fname : "/home/dave/Documents/work/data/bigdata/2018facilityclaims.csv" }
    println("attempting csv query from gui program")
    res, err := csvQuery(qSpec)
    Println(err)
    return res, err
}

//return SingleQueryResult struct with query results
func runSqlServerQuery(db *sql.DB, query string) (*SingleQueryResult,error) {
    println(query)

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
    var results[]*SingleQueryResult
    for i := range queries {
        result,err := runCachingQuery(db, queries[i], req.Mode, req.Cache)
        results = append(results, result)
        if err != nil {
            return results, err
        }
    }
    return results, nil
}

//general save function
func saveQueryFile(req *Qrequest, fullReturnData *ReturnData, full_json *[]byte) error {
    println("saving query...")
    savePath := req.FilePath
    pathStat, err := os.Stat(savePath)

    //if given a real path
    if err == nil {
        if pathStat.Mode().IsDir() {
            savePath = savePath + "sqlSaved.json"
        } //else given a real file
    } else {
        _, err := os.Stat(filepath.Dir(savePath))
        //if base path doesn't exist
        if err != nil {
            fullReturnData.Status |= DAT_BADPATH
            fullReturnData.Message = "Invalid path: " + savePath
            println("invalid path")
        } //else given new file
    }

    //save file
    if fullReturnData.Status & DAT_BADPATH == 0 {
        file, err := os.OpenFile(savePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
        defer file.Close()
        FPaths.SavePath = savePath
        if err == nil {
            //actually save the file
            if (req.FileIO & F_CSV) != 0 {
                saveCsv(file, fullReturnData)
            } else {
                file.WriteString(string(*full_json))
            }
            FPaths.Status = FP_SCHANGED
            fullReturnData.Message = "Saved to "+savePath
            println("Saved to "+savePath)
        } else {
            fullReturnData.Status = (DAT_BADPATH | DAT_IOERR)
            fullReturnData.Message = "File IO error"
            println("File IO error")
        }
    }
    return err
}

//save query to csv
func saveCsv(file *os.File, fullReturnData *ReturnData) error {
    //set up csv writer and write heading
    writer := csv.NewWriter(file)
    defer writer.Flush()
    err := writer.Write(fullReturnData.Entries[0].Colnames)
    //prepare array for each row
    var output []string
    output = make([]string, fullReturnData.Entries[0].Numcols)
    for _, value := range fullReturnData.Entries[0].Vals {
        for i,entry := range value {
            //make sure each entry is formatted well according to its type
            if entry == nil { output[i] = ""
            } else {
                switch entry.(type) {
                    //case time.Time: output[i] = entry.(time.Time).Format(time.RFC3339)
                    case time.Time: output[i] = entry.(time.Time).Format("2006-01-02 15:04:05")
                    default: output[i] = Sprint(entry)
                }
            }
        }
        err = writer.Write(output)
    }
    return err
}

//handle file opening
func openQueryFile(req *Qrequest, fullReturnData *ReturnData) ([]byte, error) {
    FPaths.OpenPath = req.FilePath
    FPaths.Status |= FP_OCHANGED
    fileData, err := ioutil.ReadFile(FPaths.OpenPath)
    if err != nil {
        fullReturnData.Status |= DAT_ERROR
        fullReturnData.Message = "Error opening file"
        println("Error opening file")
    } else {
        json.Unmarshal(fileData,&fullReturnData)
        fullReturnData.Message = "Opened file"
        println("Opened file")
    }
    full_json, err := json.Marshal(fullReturnData)
    return full_json, err
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
