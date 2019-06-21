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
)

//command line flags
var localPort = flag.String("port", "8060", "Change localhost port")
var danger = flag.Bool("danger",false, "Allow connections from non-localhost. Dangerous, only use for debugging.")
var testing = flag.Bool("test",false, "run tests and then exit")



var FPaths FilePaths
var messager chan string
var saver chan saveData
var savedLine chan bool
var fileclick chan string
var directory chan Directory
var browsersOpen = 0
var slash string

func main() {
	println("version 0.30 - 6/20/2019")
	flag.Parse()
	runTests(*testing)
	//get password and other flags
	messager = make(chan string)
	fileclick = make(chan string)
	directory = make(chan Directory)
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
			if counter > 10 { os.Exit(2) }
		}
	}()

	//launch web browser for gui
	launch("http://localhost"+port);
	<-done

}

//wrapper for csvQuery
func runCsvQuery(query string, req *Qrequest) (SingleQueryResult,error) {
	q := QuerySpecs{ queryString : query, }
	if (req.FileIO & F_CSV) != 0 { q.save = true }
	res, err := csvQuery(&q)
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
		messager <- "Finishing a query..."
		results = append(results, result)
		if err != nil {
			messager <- Sprint(err)
			return results, errors.New("Query "+Itoa(i+1)+" Error: "+Sprint(err))
		}
	}
	return results, nil
}
