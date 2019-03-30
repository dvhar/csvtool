package main
import (
    "encoding/json"
    "encoding/csv"
    "path/filepath"
    "io/ioutil"
    "regexp"
    . "strconv"
    "errors"
    . "fmt"
    "time"
    "os"
)



//general save function
func saveQueryFile(req *Qrequest, fullReturnData *ReturnData, full_json *[]byte) error {
    println("saving query...")
    savePath := req.FilePath
    pathStat, err := os.Stat(savePath)

    //if given a real path
    if err == nil {
        if pathStat.Mode().IsDir() {
            fullReturnData.Message = "Must specify a file name to save"
            return errors.New("Must specify a file name to save")
        } //else given a real file
    } else {
        _, err := os.Stat(filepath.Dir(savePath))
        //if base path doesn't exist
        if err != nil {
            fullReturnData.Status |= DAT_BADPATH
            fullReturnData.Message = "Invalid path: " + savePath
            println("invalid path")
        } //else given new file
    }

    if fullReturnData.Status & DAT_BADPATH == 0 {
        FPaths.SavePath = savePath
        //save the file after checking file path
        if (req.FileIO & F_CSV) != 0 {
            //write csv file
            println("saving postquery csv")
            err = saveCsv(fullReturnData)
        } else {
            //write json file
            println("saving postquery json")
            extension := regexp.MustCompile(`\.json$`)
            if !extension.MatchString(FPaths.SavePath) { FPaths.SavePath += `.json` }
            var file *os.File
            file, err = os.OpenFile(FPaths.SavePath, os.O_CREATE|os.O_WRONLY, 0660)
            file.WriteString(string(*full_json))
            file.Close()
            if err == nil { fullReturnData.Message = `saved to `+FPaths.SavePath }
        }
        if err == nil {
            //it worked
            FPaths.Status = FP_SCHANGED
            println("Saved to "+FPaths.SavePath)
        } else {
            //it didnt work
            fullReturnData.Status = (DAT_BADPATH | DAT_IOERR)
            fullReturnData.Message = "File IO error"
            println("File IO error")
        }
    }
    return err
}

//save query from memory to csv
func saveCsv(fullReturnData *ReturnData) error {
    //set up csv writer and write heading
    extension := regexp.MustCompile(`\.csv$`)
    if !extension.MatchString(FPaths.SavePath) { FPaths.SavePath += `.csv` }

    //loop through queries and save each to its own file
    for ii,qdata := range fullReturnData.Entries {
        var saveFile string
        if len(fullReturnData.Entries) > 1 {
            saveFile = extension.ReplaceAllString(FPaths.SavePath, `-`+Itoa(ii+1)+`.csv`)
            fullReturnData.Message = `saved to `+extension.ReplaceAllString(FPaths.SavePath, `-N.csv files.`)
        } else {
            saveFile = FPaths.SavePath
            fullReturnData.Message = `saved to `+saveFile
        }
        file, err := os.OpenFile(saveFile, os.O_CREATE|os.O_WRONLY, 0660)
        if err != nil { return err }
        writer := csv.NewWriter(file)
        defer writer.Flush()
        err = writer.Write(qdata.Colnames)
        if err != nil { return err }
        //prepare array for each row
        var output []string
        output = make([]string, qdata.Numcols)
        for _, value := range qdata.Vals {
            for i,entry := range value {
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
            if err != nil { return err }
        }
        file.Close()
    }
    return nil
}

//handle file opening
func openQueryFile(req *Qrequest, fullReturnData *ReturnData) ([]byte, error) {
    FPaths.OpenPath = req.FilePath
    FPaths.Status |= FP_OCHANGED
    fileData, err := ioutil.ReadFile(FPaths.OpenPath)
    if err != nil {
        fullReturnData.Status |= DAT_ERROR
        fullReturnData.Message = "Error opening file"
        println("Error opening file")
    } else {
        json.Unmarshal(fileData,&fullReturnData)
        fullReturnData.Message = "Opened file"
        println("Opened file")
    }
    full_json, err := json.Marshal(fullReturnData)
    return full_json, err
}

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

type Directory struct {
    Path string
    Parent string
    Files []string
    Dirs []string
}
func fileBrowser() error {
    extension := regexp.MustCompile(`\.csv$`)
    var thisDir Directory

    for path := range fileclick {

        thisDir = Directory{}

        //clean clicked path and get parent
        path := filepath.Clean(path)
        files, _ := filepath.Glob(path+slash+"*")

        thisDir.Path = path
        thisDir.Parent = filepath.Dir(path)

        //get subdirs and csv files
        for _,file := range files {
            ps, err := os.Stat(file)
            if err != nil { continue }
            if ps.Mode().IsDir() {
                println("D: "+file)
                thisDir.Dirs = append(thisDir.Dirs, file)
            } else if extension.MatchString(file) {
                println("F: "+file)
                thisDir.Files = append(thisDir.Files, file)
            }
        }

        Println(thisDir)

    }

    return nil
}
