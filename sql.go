package main
import (
    _ "github.com/denisenkom/go-mssqldb"
    "github.com/GeertJohan/go.rice"
    //"github.com/Jeffail/gabs"
    "encoding/json"
    "database/sql"
    "net/http"
    "net/url"
    "io/ioutil"
    "strings"
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

// -n to not connect to azure
// -c to not run the server
// -p to change port
var noms = flag.Bool("n", false, "Don't connect to azure")
var cmode = flag.Bool("c", false, "Run in text mode for debugging")
var port = flag.String("p", "8060", "Change port from 8060")

func main() {
    var db *sql.DB
    flag.Parse()
    if (! *noms) { db = sqlConnect() }

    //output to stdout for debugging
    if (*cmode) {

        println("running in text mode")
        entries := runQueries(db, premade("columns_abridged") +
                        premade("primaries") + 
                        premade("columns_total") + 
                        premade("columns_withkey"))
        j,_ := json.Marshal(entries)
        Println(string(j))

    //run webserver
    } else {

        println("running in server mode")
        server(db)
    }

    if (! *noms) {
        println("closing connection")
        db.Close()
    }
}

//webserver
func server(db *sql.DB) {
    http.Handle("/", http.FileServer(rice.MustFindBox("webgui/build").HTTPBox()))
    http.HandleFunc("/query", queryHandler(db))
    http.HandleFunc("/query/", queryHandler(db))
    http.HandleFunc("/premade/", premadeHandler(db))
    http.ListenAndServe(":"+*port, nil)
    //http.ListenAndServe("localhost:"+*port, nil)
}

//returns handler function for query requests from the webgui
func queryHandler(db *sql.DB) (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        type Qr struct {
            Query string
        }
        body, _ := ioutil.ReadAll(r.Body)
        println(formatRequest(r))
        println(string(body))
        var rec Qr
        json.Unmarshal(body,&rec)
        entries := runQueries(db, rec.Query)
        full_json,_ := json.Marshal(entries)
        //Printf("resp: %+v", full_json)
        println(string(full_json))
        Fprint(w, string(full_json))
    }
}

func premadeHandler(db *sql.DB) (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        println("Trying query...")
        entries := runQueries(db, premade("columns_abridged") + premade("primaries"))
        full_json,_ := json.Marshal(entries)
        Fprint(w, string(full_json))
        println("finished query.")
    }
}

//initialize database connection
func sqlConnect() (*sql.DB) {
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
    println("open connection")
    db,_ := sql.Open("mssql", connectString)
    return db
}

//return Qrows struct with query results
func runQuery(db *sql.DB, query string) *Qrows {
    //if server connection allowed
    if (! *noms) {
        rows,_ := db.Query(query)
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
        ret := &Qrows{Colnames: columnNames, Numcols: len(columnNames), Numrows: rownum, Vals: entries}
        return ret
    } else {
        ret := &Qrows{}
        return ret
    }
}

//run multiple queries deliniated by semicolon
func runQueries(db *sql.DB, query string) []*Qrows {
    if (strings.HasSuffix(query,";")) { query = query[:len(query)-1] }
    queries := strings.Split(query,";")
    var results[]*Qrows
    for i := range queries {
        results = append(results, runQuery(db,queries[i]))
    }
    return results
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
