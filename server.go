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
    "time"
    "os/exec"
    //. "strconv"
	//socketio "github.com/googollee/go-socket.io"
    "github.com/gorilla/websocket"
)

//websockets
const (
    SK_MSG = iota
    SK_PING = iota
    SK_PONG = iota
    SK_STOP = iota
    SK_DIRLIST = iota
    SK_FILECLICK = iota
)
type Client struct {
    conn *websocket.Conn
    w http.ResponseWriter
    r *http.Request
}
type sockMessage struct {
    Type int
    Text string
}
type sockDirMessage struct {
    Type int
    Dir Directory
}
//write loop for each websocket client
func (c* Client) writer(){
    ticker := time.NewTicker(time.Second)
    browsersOpen++
    defer func(){
        browsersOpen--
        c.conn.Close()
        ticker.Stop()
    }()
    var sendSock sockMessage
    var sendDirSock sockDirMessage
    var sendBytes []byte
    for {
        select {
            case msg := <-messager:
                sendSock = sockMessage{ Type: SK_MSG, Text:msg }
                sendBytes,_ = json.Marshal(sendSock)
            case <-ticker.C:
                sendSock = sockMessage{ Type: SK_PING }
                sendBytes,_ = json.Marshal(sendSock)
            case dir := <-directory:
                sendDirSock = sockDirMessage{ Type: SK_DIRLIST, Dir: dir }
                sendBytes,_ = json.Marshal(sendDirSock)
        }
        err := c.conn.WriteMessage(1, sendBytes)
        if err != nil { println("socket writer failed"); return }
    }
}
//read loop for each websocket client
func (c* Client) reader(){
    var message sockMessage
    for {
        _, messageBytes, err := c.conn.ReadMessage()
        if err != nil {
            if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
                Printf("error: %v", err)
            }
            return
        }
        json.Unmarshal(messageBytes, &message)
        //Printf("%+v\n",message)
        switch message.Type {
            case SK_STOP:
                if active { stop = 1 }
            case SK_FILECLICK:
                go fileBrowser(message.Text)
        }
    }
}

//each new client gets a websocket
func socketHandler() (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        upgrader := websocket.Upgrader{
            ReadBufferSize:  2048,
            WriteBufferSize: 2048,
        }
        sconn, err := upgrader.Upgrade(w, r, nil)
        if err != nil {
            Println("upgrade:", err)
            return
        }
        client := &Client{ w : w, r : r, conn: sconn }
        go client.writer()
        go client.reader()
    }
}

//webserver
func httpserver(serverUrl string, done chan bool) {

    http.Handle("/", http.FileServer(rice.MustFindBox("webgui/build").HTTPBox()))
    http.HandleFunc("/query/", queryHandler())
    http.HandleFunc("/info/", infoHandler())
    http.HandleFunc("/socket/", socketHandler())
    http.ListenAndServe(serverUrl, nil)
    done <- true
}

//want only one set of results in memory at once, so global var
var retData ReturnData
//returns handler function for query requests from the webgui
func queryHandler() (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {

        body, _ := ioutil.ReadAll(r.Body)
            println(formatRequest(r))
            println(string(body))
        var req Qrequest
        var err error
        retData = ReturnData{}
        json.Unmarshal(body,&req)
        retData.Status = DAT_BLANK
        retData.OriginalQuery = req.Query

        //attempt query
        println("requesting query")
        retData.Entries,err = runQueries(&req)
        if (req.FileIO & F_CSV) != 0 { saver <- chanData{Type : CH_DONE} }
        if err != nil {
            retData.Status |= DAT_ERROR
            retData.Message = Sprint(err)
        } else {
            retData.Status |= DAT_GOOD
            messager <- "Query successful. Returning data"
        }

        full_json,_ := json.Marshal(retData)

        //update json with save message
        rowLimit(&retData)
        if retData.Clipped { messager <- "Showing only top 1000" }
        full_json,_ = json.Marshal(retData)
        Fprint(w, string(full_json))
        full_json = []byte("")
        retData = ReturnData{}
        runtime.GC()
    }
}

//limit the amount of rows returned to the browser because browsers are slow
func rowLimit(retData *ReturnData) {
    for i, query := range retData.Entries {
        if query.Numrows > 1000 {
            retData.Entries[i].Vals = query.Vals[:1000]
            retData.Clipped = true
            runtime.GC()
        }
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

