package main
import (
    _ "github.com/denisenkom/go-mssqldb"
    //"github.com/Jeffail/gabs"
    "encoding/base64"
    "encoding/json"
    "encoding/csv"
    "path/filepath"
    "database/sql"
    "crypto/sha1"
    "io/ioutil"
    "net/url"
    "regexp"
    "strings"
    . "strconv"
    "errors"
    . "fmt"
    "time"
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
type chanData struct {
    Message string
    Status int
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
var c chan chanData

func main() {
    //get password and other flags
    flag.Parse()
    c = make(chan chanData)
    if *dbpass == "" { *dbpass = os.Getenv("MSSQL_CLI_PASSWORD") }

    //initialize query data cache and file paths
    Qcache = make(map[string]*SingleQueryResult)
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
    httpserver(serverUrl)
    //<-done

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
            fullReturnData.Message = "Must specify a file name to save"
            return errors.New("Must specify a file name to save")
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

    if fullReturnData.Status & DAT_BADPATH == 0 {
        FPaths.SavePath = savePath
        //save the file after checking file path
        if (req.FileIO & F_CSV) != 0 {
            //write csv file
            err = saveCsv(fullReturnData)
        } else {
            //write json file
            extension := regexp.MustCompile(`\.json$`)
            if !extension.MatchString(FPaths.SavePath) { FPaths.SavePath += `.json` }
            var file *os.File
            file, err = os.OpenFile(FPaths.SavePath, os.O_CREATE|os.O_WRONLY, 0660)
            file.WriteString(string(*full_json))
            file.Close()
            if err == nil { fullReturnData.Message = `saved to `+FPaths.SavePath }
        }
        if err == nil {
            //it worked
            FPaths.Status = FP_SCHANGED
            println("Saved to "+savePath)
        } else {
            //it didnt work
            fullReturnData.Status = (DAT_BADPATH | DAT_IOERR)
            fullReturnData.Message = "File IO error"
            println("File IO error")
        }
    }
    return err
}

//save query to csv
func saveCsv(fullReturnData *ReturnData) error {
    //set up csv writer and write heading
    extension := regexp.MustCompile(`\.csv$`)
    if !extension.MatchString(FPaths.SavePath) { FPaths.SavePath += `.csv` }

    //loop through queries and save each to its own file
    for ii,qdata := range fullReturnData.Entries {
        var saveFile string
        if len(fullReturnData.Entries) > 1 {
            saveFile = extension.ReplaceAllString(FPaths.SavePath, `-`+Itoa(ii+1)+`.csv`)
            fullReturnData.Message = `saved to `+extension.ReplaceAllString(FPaths.SavePath, `-N.csv files.`)
        } else {
            saveFile = FPaths.SavePath
            fullReturnData.Message = `saved to `+saveFile
        }
        file, err := os.OpenFile(saveFile, os.O_CREATE|os.O_WRONLY, 0660)
        if err != nil { return err }
        writer := csv.NewWriter(file)
        defer writer.Flush()
        err = writer.Write(qdata.Colnames)
        if err != nil { return err }
        //prepare array for each row
        var output []string
        output = make([]string, qdata.Numcols)
        for _, value := range qdata.Vals {
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
            if err != nil { return err }
        }
    }
    return nil
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
