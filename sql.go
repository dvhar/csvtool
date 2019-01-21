package main
import (
   _ "github.com/denisenkom/go-mssqldb"
   "github.com/Jeffail/gabs"
   "database/sql"
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
    entries := runQuery(db, "select * FROM INFORMATION_SCHEMA.Columns")
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
