package main
import (
	"regexp"
	"runtime"
	"strings"
	"errors"
	"time"
	. "fmt"
	. "strconv"
	"flag"
	"os"
	"bufio"
)



var flags Flags
var FPaths FilePaths
var messager chan string
var saver chan saveData
var savedLine chan bool
var fileclick chan string
var browsersOpen = 0
var slash string
var printer Printer

func main() {

	flags.localPort = flag.String("p", "8060", "Change localhost port")
	flags.danger = flag.Bool("l",false, "Allow connections from non-localhost")
	flags.persistent = flag.Bool("s",false, "Don't stop server when browser closes")
	flags.command = flag.String("c","", "Run query from command line argument")
	flags.version = flag.Bool("v",false, "Show version")
	flag.Parse()

	readStdin()
	if *flags.version { println("version 0.6 - 11/9/2019"); os.Exit(0) }

	messager = make(chan string)
	fileclick = make(chan string)
	saver = make(chan saveData)
	savedLine = make(chan bool)
	go realtimeCsvSaver()

	//initialize file paths
	cwd, err := os.Getwd()
	if err == nil {
		if runtime.GOOS == "windows" { slash = "\\" } else { slash = "/" }
		FPaths.OpenPath = cwd + slash
		FPaths.SavePath  = cwd + slash
		FPaths.Status = 0
	} else {
		FPaths.Status = FP_OERROR | FP_SERROR
	}

	//set up server url
	host := "localhost"
	port := ":" + *flags.localPort
	if *flags.danger { host = "" }
	serverUrl := host + port


	//start server
	runCommand()
	done := make(chan bool)
	go httpserver(serverUrl, done)

	//exit program if it goes 10 seconds without being viewed in a browser
	go func(){
		if *flags.persistent { return }
		ticker := time.NewTicker(time.Second)
		counter := 0
		for {
			<-ticker.C
			if browsersOpen < 1 {
				counter++
			} else {
				counter = 0
			}
			if counter > 60 { os.Exit(2) }
		}
	}()

	//launch web browser for gui
	launch("http://localhost"+port);
	<-done

}

//wrapper for CsvQuery
func runCsvQuery(query string, req *webQueryRequest) (SingleQueryResult,error) {
	q := QuerySpecs{ QueryString : query, }
	if (req.FileIO & F_CSV) != 0 { q.save = true }
	res, err := CsvQuery(&q)
	res.Query = query;
	return res, err
}


//run webQueryRequest with multiple queries deliniated by semicolon
func runQueries(req *webQueryRequest) ([]SingleQueryResult, error) {
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
			Message : req.SavePath,
		}
	}
	//run queries in a loop
	var results[]SingleQueryResult
	var result SingleQueryResult
	var err error
	for i := range queries {
		//run query
		result, err = runCsvQuery(queries[i], req)
		message("Finishing a query...")
		results = append(results, result)
		if err != nil {
			message(Sprint(err))
			return results, errors.New("Query "+Itoa(i+1)+" Error: "+Sprint(err))
		}
	}
	return results, nil
}

func runCommand() {
	if *flags.command == "" { return }
	q := QuerySpecs{ QueryString : *flags.command, save : true } //sends output to stdout
	saver <- saveData{ Type : CH_SAVPREP }
	CsvQuery(&q)
	saver <- saveData{ Type : CH_NEXT }
	os.Exit(0)
}

func readStdin() {
	fi,_ := os.Stdin.Stat()
	if fi.Mode() & os.ModeNamedPipe != 0 {
		reader := bufio.NewReader(os.Stdin)
		buf := make([]byte,10000)
		reader.Read(buf)
		*flags.command = string(buf)
	}
}
