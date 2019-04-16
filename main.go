package main
import (
    //_ "github.com/denisenkom/go-mssqldb"
    "regexp"
    "runtime"
    "strings"
    "errors"
    "time"
    . "fmt"
    . "strconv"
    "flag"
    "os"
)

//command line flags
var localPort = flag.String("port", "8060", "Change localhost port")
var danger = flag.Bool("danger",false, "Allow connections from non-localhost. Dangerous, only use for debugging.")


//one SingleQueryResult struct holds the results of one query
type SingleQueryResult struct {
    Numrows int
    Numcols int
    Types []int
    Colnames []string
    Pos []int
    Vals [][]interface{}
    Status int
    Message string
    Query string
}

//channel data
const (
    CH_HEADER = iota
    CH_ROW = iota
    CH_DONE = iota
    CH_NEXT = iota
    CH_SAVPREP = iota
)
type saveData struct {
    Message string
    Number int
    Type int
    Header []string
    Row *[]interface{}
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
    Entries []SingleQueryResult
    Status int
    Message string
    OriginalQuery string
    Clipped bool
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
    RtSavePath string
    OpenPath string
    CsvPath string
    Status int
}
//struct that matches incoming json requests
type Qrequest struct {
    Query string
    Qamount int
    FileIO int
    FilePath string
    CsvFile string
}

var FPaths FilePaths
var messager chan string
var saver chan saveData
var savedLine chan bool
var fileclick chan string
var directory chan Directory
var browsersOpen = 0
var slash string

func main() {
    println("version 0.11 - 4/14/2019")
    //get password and other flags
    flag.Parse()
    messager = make(chan string)
    fileclick = make(chan string)
    directory = make(chan Directory)
    saver = make(chan saveData)
    savedLine = make(chan bool)
    go realtimeCsvSaver()

    //initialize file paths
    cwd, err := os.Getwd()
    if err == nil {
        switch runtime.GOOS {
            case "windows":
                slash = "\\"
            case "darwin": fallthrough
            default:
                slash = "/"
        }
        FPaths.OpenPath = cwd + slash
        FPaths.SavePath  = cwd + slash
        FPaths.Status = 0
    } else {
        FPaths.Status = FP_OERROR | FP_SERROR
    }

    //set up server url
    println("Starting server")
    host := "localhost"
    port := ":" + *localPort
    if *danger { host = "" }
    serverUrl := host + port


    //start server
    done := make(chan bool)
    go httpserver(serverUrl, done)

    //exit program if it goes 5 seconds without being viewed in a browser
    go func(){
        ticker := time.NewTicker(time.Second)
        counter := 0
        for {
            <-ticker.C
            if browsersOpen < 1 {
                counter++
            } else {
                counter = 0
            }
            if counter > 5 { os.Exit(2) }
        }
    }()

    //launch web browser for gui
    launch("http://localhost"+port);
    <-done

}

//wrapper for csvQuery
func runCsvQuery(query string, req *Qrequest) (SingleQueryResult,error) {
    qSpec := QuerySpecs{
        Qstring : query,
        DistinctIdx : -1,
    }
    if (req.FileIO & F_CSV) != 0 { qSpec.Save = true }
    println("attempting csv query from gui program")
    res, err := csvQuery(&qSpec)
    res.Query = query;
    return res, err
}


//run Qrequest with multiple queries deliniated by semicolon
func runQueries(req *Qrequest) ([]SingleQueryResult, error) {
    query := req.Query
    //remove uneeded characters from end of string
    ending := regexp.MustCompile(`;\s*$`)
    query = ending.ReplaceAllString(query, ``)
    queries := strings.Split(strings.Replace(query,"\\n","",-1),";")
    req.Qamount = len(queries)
    //send info to realtime saver
    if (req.FileIO & F_CSV) != 0 {
        saver <- saveData{
            Number : req.Qamount,
            Type : CH_SAVPREP,
            Message : req.FilePath,
        }
    }
    //run queries in a loop
    var results[]SingleQueryResult
    var result SingleQueryResult
    var err error
    for i := range queries {
        //run query
        result, err = runCsvQuery(queries[i], req)
        results = append(results, result)
        if err != nil {
            messager <-  Sprint(err)
            return results, errors.New("Query "+Itoa(i+1)+" Error: "+Sprint(err))
        }
    }
    return results, nil
}
