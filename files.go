package main
import (
    "encoding/csv"
    "path/filepath"
    "regexp"
    . "strconv"
    "errors"
    . "fmt"
    "time"
    "os"
)


//use channel to save files directly from query without holding results in memory
func realtimeCsvSaver() {

    state := 0
    numTotal := 0
    numRecieved := 0
    extension := regexp.MustCompile(`\.csv$`)
    var file *os.File
    var err error
    var writer *csv.Writer
    var output []string

    for c := range saver {
        switch c.Type {
            case CH_SAVPREP:
                println("got saver prep")
                err = pathChecker(c.Message)
                if err == nil {
                    FPaths.RtSavePath = FPaths.SavePath
                    numTotal = c.Number
                    numRecieved = 0
                    state = 1
                }

            case CH_HEADER:
                println("got saver header")
                if state == 1 {
                    println("processed saver header")
                    numRecieved++
                    if numTotal > 1 {
                        FPaths.RtSavePath = extension.ReplaceAllString(FPaths.SavePath, `-`+Itoa(numRecieved)+`.csv`)
                    }
                    file, err = os.OpenFile(FPaths.RtSavePath, os.O_CREATE|os.O_WRONLY, 0660)
                    writer = csv.NewWriter(file)
                    err = writer.Write(c.Header)
                    output = make([]string, len(c.Header))
                    state = 2
                    savedLine <- true
                }

            case CH_ROW:
                if state == 2 {
                    for i,entry := range *(c.Row) {
                        //make sure each entry is formatted well according to its type
                        if entry == nil { output[i] = ""
                        } else {
                            switch entry.(type) {
                                //case time.Time: output[i] = entry.(time.Time).Format(time.RFC3339)
                                case time.Time: output[i] = entry.(time.Time).Format("2006-01-02 15:04:05")
                                default: output[i] = Sprint(entry)
                            }
                        }
                    }
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
            messager <- "Must specify a file name to save"
            return errors.New("Must specify a file name to save")
        } //else given a real file
    } else {
        _, err := os.Stat(filepath.Dir(savePath))
        //if base path doesn't exist
        if err != nil {
            messager <- "Invalid path: " + savePath
            return errors.New("Invalid path")
        } //else given new file
    }
    //set realtime save paths
    FPaths.SavePath = savePath
    extension := regexp.MustCompile(`\.csv$`)
    if !extension.MatchString(FPaths.SavePath) { FPaths.SavePath += `.csv` }
    return nil
}

//payload type sent to the browser
type Directory struct {
    Path string
    Parent string
    Files []string
    Dirs []string
}
//send directory payload to socket writer when given a path
func fileBrowser(path string) {
    extension := regexp.MustCompile(`\.csv$`)

    //clean directory path, get parent, and prepare output
    path = filepath.Clean(path)
    files, _ := filepath.Glob(path+slash+"*")
    _, err := os.Open(path)
    if err != nil {
        messager <- "invalid path: "+path
        return
    }
    thisDir := Directory{Path: path, Parent: filepath.Dir(path)}

    //get subdirs and csv files
    for _,file := range files {
        ps, err := os.Stat(file)
        if err != nil { continue }
        if ps.Mode().IsDir() {
            thisDir.Dirs = append(thisDir.Dirs, file)
        } else if extension.MatchString(file) {
            thisDir.Files = append(thisDir.Files, file)
        }
    }

    directory <- thisDir
}
