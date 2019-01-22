package main
import (
   _ "github.com/denisenkom/go-mssqldb"
   "github.com/Jeffail/gabs"
   "database/sql"
   "strings"
   "net/url"
   . "fmt"
)

func main() {
    login := "dfhntz"
    pass := "poop"
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

    println("Trying query...")
    entries := runQueries(db, premade("columns") + premade("primaries"))
    full_json,_ := gabs.Consume(entries)
    println(full_json.StringIndent(""," "))

    println("closing connection")
    db.Close()
}

//returns an array of maps with the query results
func runQuery(db *sql.DB, query string) []map[string]interface{} {
    rows,_ := db.Query(query)
    columnNames,_ := rows.Columns()
    columnValues := make([]interface{}, len(columnNames))
    columnPointers := make([]interface{}, len(columnNames))
    for i := 0; i < len(columnNames); i++ {
        columnPointers[i] = &columnValues[i]
    }
    var entry map[string]interface{}
    var entries[]map[string]interface{}
    for rows.Next() {
        rows.Scan(columnPointers...)
        entry = make(map[string]interface{})
        for i := 0; i < len(columnNames); i++ {
            entry[columnNames[i]] = columnValues[i]
        }
        entries = append(entries,entry)
    }
    return entries
}

//run multiple queries deliniated by semicolon
func runQueries(db *sql.DB, query string) [][]map[string]interface{} {
    if (strings.HasSuffix(query,";")) { query = query[:len(query)-1] }
    queries := strings.Split(query,";")
    var results[][]map[string]interface{}
    for i := range queries {
        results = append(results, runQuery(db,queries[i]))
    }
    return results
}

//some useful premade queries
func premade(request string ) (string) {
    switch request {
        case "columns":
            return "select * FROM INFORMATION_SCHEMA.Columns;"
        case "primaries":
            return `select col.column_name, tab.table_name, tab.constraint_type, col.constraint_name
                    FROM   INFORMATION_SCHEMA.constraint_column_usage as col
                    join INFORMATION_SCHEMA.table_constraints as tab
                    on col.constraint_name = tab.constraint_name
                    where tab.constraint_type = 'primary key'
                    and tab.table_name = col.table_name;`
        default:
            return  ""
    }
}
