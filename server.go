package main
import (
    _ "github.com/denisenkom/go-mssqldb"
    "github.com/GeertJohan/go.rice"
    //"github.com/Jeffail/gabs"
    "encoding/json"
    "io/ioutil"
    "net/http"
    "runtime"
    "strings"
    . "fmt"
    //"time"
    "os/exec"
    //. "strconv"
	//socketio "github.com/googollee/go-socket.io"
    "github.com/gorilla/websocket"
)

//websockets
var upgrader = websocket.Upgrader{} // use default options
func socketHandler() (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        output, err := upgrader.Upgrade(w, r, nil)
        if err != nil {
            Println("upgrade:", err)
            return
        }
        defer output.Close()
        for c := range messager {
            err = output.WriteMessage(1, []byte(c))
        }
    }
}

//webserver
//func server(serverUrl string, done chan bool) {
func httpserver(serverUrl string) {

    http.HandleFunc("/socket", socketHandler())
    http.HandleFunc("/socket/", socketHandler())

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
            if (req.FileIO & F_CSV) != 0 { saver <- chanData{Type : CH_DONE} }
            if err != nil {
                fullReturnData.Status |= DAT_ERROR
            } else {
                fullReturnData.Status |= DAT_GOOD
                fullReturnData.Message = "Query successful"
            }
        }

        fullReturnData.Entries = entries
        full_json,_ := json.Marshal(fullReturnData)

        //save queries if not saving from one csv to another
        if (req.FileIO & F_SAVE)!=0 && ((req.FileIO & F_JSON)!=0 || req.Mode=="MSSQL") {
            saveQueryFile(&req, &fullReturnData, &full_json)
        }

        //update json with save message
        rowLimit(&fullReturnData)
        if fullReturnData.Clipped { fullReturnData.Message += ". Showing only top 1000" }
        messager <- fullReturnData.Message
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
            //fullReturnData.Entries[i].Numrows = 1000
            fullReturnData.Clipped = true
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

