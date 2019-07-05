package main
import (
	"encoding/csv"
	"path/filepath"
	"regexp"
	. "strconv"
	"errors"
	. "fmt"
	"os"
)


//use channel to save files directly from query without holding results in memory
func realtimeCsvSaver() {

	state := 0
	numTotal := 0
	numRecieved := 0
	extension := regexp.MustCompile(`\.csv$`)
	var savePath string
	var file *os.File
	var err error
	var writer *csv.Writer
	var output []string

	for c := range saver {
		switch c.Type {
		case CH_SAVPREP:
			err = pathChecker(c.Message)
			if err == nil {
				savePath = FPaths.SavePath
				Println("Saving to ",savePath)
				numTotal = c.Number
				numRecieved = 0
				state = 1
			} else {
				messager <- Sprint(err)
			}

		case CH_HEADER:
			if state == 1 {
				numRecieved++
				if numTotal > 1 {
					savePath = extension.ReplaceAllString(FPaths.SavePath, `-`+Itoa(numRecieved)+`.csv`)
				}
				file, err = os.OpenFile(savePath, os.O_CREATE|os.O_WRONLY, 0660)
				writer = csv.NewWriter(file)
				err = writer.Write(c.Header)
				output = make([]string, len(c.Header))
				state = 2
				savedLine <- true
			}

		case CH_ROW:
			if state == 2 {
				for i,entry := range *(c.Row) { output[i] = entry.String() }
				err = writer.Write(output)
				savedLine <- true
			}

		case CH_NEXT:
			writer.Flush()
			file.Close()
			state = 1

		case CH_DONE:
			state = 0
		}
		if err != nil { messager <- Sprint(err) }
	}
}



func pathChecker(savePath string) error {

	pathStat, err := os.Stat(savePath)
	//if given a real path
	if err == nil {
		if pathStat.Mode().IsDir() {
			return errors.New("Must specify a file name to save")
		} //else given a real file
	} else {
		_, err := os.Stat(filepath.Dir(savePath))
		//if base path doesn't exist
		if err != nil {
			return errors.New("Invalid path: "+savePath)
		} //else given new file
	}
	//set savepath and append csv extension if needed
	FPaths.SavePath = savePath
	extension := regexp.MustCompile(`\.csv$`)
	if !extension.MatchString(FPaths.SavePath) { FPaths.SavePath += `.csv` }
	return nil
}

//payload type sent to and from the browser
type Directory struct {
	Path string
	Parent string
	Mode string
	Files []string
	Dirs []string
}
//send directory payload to socket writer when given a path
func fileBrowser(pathRequest Directory) {
	extension := regexp.MustCompile(`\.csv$`)
	hiddenDir := regexp.MustCompile(`/\.[^/]+$`)

	//clean directory path, get parent, and prepare output
	path := filepath.Clean(pathRequest.Path)
	files, _ := filepath.Glob(path+slash+"*")
	_, err := os.Open(path)
	if err != nil {
		messager <- "invalid path: "+path
		return
	}
	thisDir := Directory{Path: path+slash, Parent: filepath.Dir(path), Mode: pathRequest.Mode}

	//get subdirs and csv files
	for _,file := range files {
		ps, err := os.Stat(file)
		if err != nil { continue }
		if ps.Mode().IsDir() && !hiddenDir.MatchString(file) {
			thisDir.Dirs = append(thisDir.Dirs, file+slash)
		} else if extension.MatchString(file) {
			thisDir.Files = append(thisDir.Files, file)
		}
	}

	directory <- thisDir
}
