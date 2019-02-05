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
    "strings"
    "errors"
    . "fmt"
    "flag"
    "os"
)

//one Qrows struct holds the results of one query
type Qrows struct {
    Numrows int
    Numcols int
    Colnames []string
    Vals [][]interface{}
}
type ReturnData struct {
    Entries []*Qrows
    Status int
}

//TODO: find out if program will run on multiple databases. Will need cache for each db
var Qcache map[string]*Qrows

// -n to not connect to azure
// -c to not run the server
// -p to change port
var noms = flag.Bool("n", false, "Don't connect to azure")
var cmode = flag.Bool("c", false, "Run in text mode for debugging")
var port = flag.String("p", "8060", "Change port from 8060")

func main() {
    var db *sql.DB
    var er error
    flag.Parse()
    Qcache = make(map[string]*Qrows)

    //if connecting to database
    if (! *noms) {
        db,er = sqlConnect()
    }

    println("Starting server")
    server(db,er)


    //clost database connection if there is one
    if (! *noms) {
        println("closing connection")
        db.Close()
    }
}

//webserver
func server(db *sql.DB, er error) {
    http.Handle("/", http.FileServer(rice.MustFindBox("webgui/build").HTTPBox()))
    http.HandleFunc("/query", queryHandler(db,er))
    http.HandleFunc("/query/", queryHandler(db,er))
    http.HandleFunc("/premade", premadeHandler(db))
    http.HandleFunc("/premade/", premadeHandler(db))
    http.ListenAndServe(":"+*port, nil)
    //http.ListenAndServe("localhost:"+*port, nil)
}

//returns handler function for query requests from the webgui
func queryHandler(db *sql.DB, er error) (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        type Qr struct {
            Query string
        }
        body, _ := ioutil.ReadAll(r.Body)
        println(formatRequest(r))
        println(string(body))
        var rec Qr
        var entries []*Qrows
        var fullData ReturnData
        var err error
        json.Unmarshal(body,&rec)
        fullData.Status = 0

        //return null query if no connection
        if er != nil {
            println("no database connection")
            entries = append(entries,&Qrows{})

        //attempt query if there is a connection
        } else {
            println("requesting query")
            entries,err = runQueries(db, rec.Query)
            if err != nil {
                fullData.Status |= 1
            }
        }

        fullData.Entries = entries
        full_json,_ := json.Marshal(fullData)
        //Printf("resp: %+v", full_json)
        //println(string(full_json))
        Fprint(w, string(full_json))
    }
}

func premadeHandler(db *sql.DB) (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        println("Trying query...")
        entries,_ := runQueries(db, premade("columns_abridged") + premade("primaries"))
        full_json,_ := json.Marshal(entries)
        Fprint(w, string(full_json))
        println("finished query.")
    }
}

//initialize database connection
func sqlConnect() (*sql.DB, error) {
    login := "dfhntz"
    pass := os.Getenv("MSSQL_CLI_PASSWORD")
    server := "dfhntz.database.windows.net"
    dbname := "testdb"
    port := 1433
    query := url.Values{}
    query.Add("database",dbname)
    query.Add("connection timeout","30")
    u := &url.URL{
        Scheme:   "sqlserver",
        User:     url.UserPassword(login, pass),
        Host:     Sprintf("%s:%d", server, port),
        RawQuery: query.Encode(),
    }
    connectString := u.String()
    db,err := sql.Open("mssql", connectString)
    if err != nil {
        println("connection error")
    } else {
        println("db connection successful")
    }
    return db, err
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

    //if server connection allowed
    if (! *noms) {

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
            return &Qrows{Colnames: columnNames, Numcols: len(columnNames), Numrows: rownum, Vals: entries}, nil
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
    queries := strings.Split(query,";")
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
