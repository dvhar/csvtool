package main
import (
   _ "github.com/denisenkom/go-mssqldb"
   "database/sql"
   "net/url"
   "log"
   . "fmt"
)

func main() {
   login := "dfhntz"
   pass := "poop"
   server := "dfhntz.database.windows.net"
   dbname := "testdb"
   port := 1433
   var tab_names string;

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
   db, err := sql.Open("mssql", connectString)
   defer db.Close()
   println ("Open Error:" , err)
   if err != nil { log.Fatal(err) }

   println("Trying query...")
   rows, err := db.Query("select distinct table_name FROM INFORMATION_SCHEMA.Columns")
   if err != nil { log.Fatal(err) }

   println ("names of tables:")
   for rows.Next() {
     rows.Scan(&tab_names)
     Printf ("\t%s\n",tab_names)
   }

   println("closing connection")
   db.Close()
}
