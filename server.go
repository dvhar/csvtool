package main
import (
	"github.com/GeertJohan/go.rice"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	. "fmt"
	"time"
	"os/exec"
	. "strconv"
	"github.com/gorilla/websocket"
)
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
				go fileBrowser(Directory{Path : message.Text, Mode : message.Mode})
		}
	}
}

//each new client gets a websocket
func socketHandler() (func(http.ResponseWriter, *http.Request)) {
	return func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
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
	println("Starting server at "+serverUrl)
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
		var req webQueryRequest
		var err error
		retData = ReturnData{}
		json.Unmarshal(body,&req)
		retData.Status = DAT_BLANK
		retData.OriginalQuery = req.Query

		println("attempting queries")
		retData.Entries,err = runQueries(&req)
		successMessage := "Query successful. Returning data"
		if (req.FileIO & F_CSV) != 0 {
			saver <- saveData{Type : CH_DONE}
			successMessage = "Saved to "+FPaths.SavePath
		}

		println("finished queries")
		if err != nil {
			retData.Status |= DAT_ERROR
			retData.Message = Sprint(err)
		} else {
			retData.Status |= DAT_GOOD
			message(successMessage)
		}

		rowLimit(&retData)
		println("finished row limit")
		if (retData.Status & DAT_GOOD)!=0 && retData.Clipped && req.FileIO == 0 { message("Showing only top "+Itoa(maxLimit)) }
		returnJSON,_ := json.Marshal(retData)
		retData = ReturnData{}
		println("marshalled json")
		Fprint(w, string(returnJSON))
		println("sent data to http writer")
		returnJSON = []byte("")
		runtime.GC()
		println("all done")
	}
}

//limit the amount of rows returned to the browser because browsers are slow
var maxLimit int
func rowLimit(retData *ReturnData) {
	maxLimit = 0
	for i, query := range retData.Entries {
		if query.Numrows > query.ShowLimit {
			if query.ShowLimit > maxLimit { maxLimit = query.ShowLimit }
			if query.ShowLimit > len(query.Vals) { continue }
			retData.Entries[i].Vals = query.Vals[:query.ShowLimit]
			retData.Clipped = true
			runtime.GC()
		}
	}
}

//make this more general-purpose for retrieving misc info
func infoHandler() (func(http.ResponseWriter, *http.Request)) {
	type InfoRequest struct {
		Info string `json:"info"`
	}
	type PathInfo struct {
		SavePath string
		OpenPath string
		Status int
	}
	type HistoryUnit struct {
		Query string `json:"query"`
	}
	type StateInfo struct {
		HaveInfo bool         `json:"haveInfo"`
		CurrentQuery string   `json:"currentQuery"`
		History []HistoryUnit `json:"history"`
		OpenDirList Directory `json:"openDirList"`
		SaveDirList Directory `json:"saveDirList"`
	}
	type StateSetReq struct {
		InfoRequest
		StateInfo
	}
	var state StateInfo

	return func(w http.ResponseWriter, r *http.Request) {
		var req InfoRequest
		var ret interface{}
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body,&req)
		Println("info request:",string(body),req)

		switch req.Info {
			case "paths":
				var pathRet PathInfo
				pathRet.SavePath = FPaths.SavePath
				pathRet.OpenPath = FPaths.OpenPath
				pathRet.Status = FPaths.Status
				ret = pathRet
			case "setState":
				json.Unmarshal(body,&state)
				Printf("%s\n\n%+v\n","new state:",state)
			case "getState":
				ret = state
		}

		returnJSON,_ := json.Marshal(ret)
		Fprint(w, string(returnJSON))
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

