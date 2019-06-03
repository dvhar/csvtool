package main
import (
  . "fmt"
  "encoding/csv"
  "os"
  s "strings"
  d "github.com/araddon/dateparse"
  . "strconv"
  "time"
  "sort"
  "io"
  "bytes"
)

var stop int
var active bool

//Random access csv reader
type LineReader struct {
    Results []interface{}
    Types []int
    ValPositions []ValPos
    LineBytes []byte
    Limit int
    MaxLineSize int
    Pos int64
    PrevPos int64
    LineBuffer bytes.Buffer
    Tee io.Reader
    CsvReader *csv.Reader
    ByteReader *bytes.Reader
    Fp *os.File
}
type ValPos struct {
    Pos int64
    Val interface{}
}
func (l*LineReader) SavePos(colNo int) {
    l.ValPositions = append(l.ValPositions, ValPos{l.PrevPos, l.Results[colNo]})
}
func (l*LineReader) PrepareReRead() {
    l.LineBytes = make([]byte, l.MaxLineSize)
    l.ByteReader = bytes.NewReader(l.LineBytes)
}
func (l*LineReader) Init(q *QuerySpecs, f string) {
    l.Types = q.files[f].types
    l.Fp,_ = os.Open(q.files[f].fname)
    l.ValPositions = make([]ValPos,0)
    l.Tee = io.TeeReader(l.Fp, &l.LineBuffer)
    l.CsvReader = csv.NewReader(l.Tee)
    l.Results = make([]interface{}, q.files[f].width)
    if q.quantityLimit == 0 { l.Limit = 1<<62 } else { l.Limit = q.quantityLimit }
    l.Read()
}
func (l*LineReader) convertLine(inline *[]string) {
    for i,cell := range (*inline) {
        cell = s.TrimSpace(cell)
        if s.ToLower(cell) == "null" || cell == "" { l.Results[i] = nil
        } else {
            switch l.Types[i] {
                case T_INT:    l.Results[i],_ = Atoi(cell)
                case T_FLOAT:  l.Results[i],_ = ParseFloat(cell,64)
                case T_DATE:   l.Results[i],_ = d.ParseAny(cell)
                case T_NULL:   fallthrough
                case T_STRING: l.Results[i] = cell
            }
        }
    }
}
func (l*LineReader) Read() ([]interface{},error) {
    line, err := l.CsvReader.Read()
    l.LineBytes, _ = l.LineBuffer.ReadBytes('\n')
    size := len(l.LineBytes)
    if l.MaxLineSize < size { l.MaxLineSize = size }
    l.PrevPos = l.Pos
    l.Pos += int64(size)
    l.convertLine(&line)
    return l.Results, err
}
func (l*LineReader) ReadAt(lineNo int) ([]interface{},error) {
    l.Fp.ReadAt(l.LineBytes, l.ValPositions[lineNo].Pos)
    l.ByteReader.Seek(0,0)
    l.CsvReader = csv.NewReader(l.ByteReader)
    line, err := l.CsvReader.Read()
    l.convertLine(&line)
    return l.Results, err
}

//run csv query
func csvQuery(q *QuerySpecs) (SingleQueryResult, error) {

    //parse and do stuff that only needs to be done once
    var err error
    q.tree,err = parseQuery(q)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    if q.save { saver <- saveData{Type : CH_HEADER, Header : q.colSpec.NewNames}; <-savedLine }
    q.showLimit = 20000 / len(q.colSpec.NewNames)
    active = true

    //prepare output
    res:= SingleQueryResult{
        Colnames : q.colSpec.NewNames,
        Numcols: q.colSpec.NewWidth,
        Types: q.colSpec.NewTypes,
        Pos: q.colSpec.NewPos,
        ShowLimit : q.showLimit,
    }

    //prepare reader and run query
    var reader LineReader
    reader.Init(q, "file1")
    defer func(){ active=false; if q.save {saver <- saveData{Type:CH_NEXT}}; reader.Fp.Close() }()
    if q.sortWay == 0 {
        err = normalQuery(q, &res, &reader)
    } else {
        err = orderedQuery(q, &res, &reader)
    }
    if err != nil { Println(err); return SingleQueryResult{}, err }
    return res, nil
}

//retrieve results on first pass
func normalQuery(q *QuerySpecs, res *SingleQueryResult, reader *LineReader) error {
    rowsChecked := 0
    stop = 0
    distinctCheck := make(map[interface{}]bool)
    for ;res.Numrows<reader.Limit; {
        if stop == 1 { stop = 0; messager <- "query cancelled"; break }

        //read line from csv file
        fromRow,err := reader.Read()
        if err != nil {break}

        //find matches and retrieve results
        match,err := evalWhere(q, &fromRow)
        if err != nil {return err}
        if match && evalDistinct(q, &fromRow, distinctCheck) {
            execSelect(q, res, &fromRow)
            res.Numrows++;
        }

        //periodic updates
        rowsChecked++
        if rowsChecked % 10000 == 0 { messager <- "Scanning line "+Itoa(rowsChecked)+", "+Itoa(res.Numrows)+" matches so far" }
    }
    return nil
}


//see if row has distinct value if looking for one
func evalDistinct(q *QuerySpecs, fromRow *[]interface{}, distinctCheck map[interface{}]bool) bool {
    if q.distinctIdx < 0 { return true }
    compVal := (*fromRow)[q.distinctIdx]
    //ok means not distinct
    _,ok := distinctCheck[compVal]
    if ok {
        return false
    } else {
        distinctCheck[compVal] = true
    }
    return true
}

//run ordered query
func orderedQuery(q *QuerySpecs, res *SingleQueryResult, reader *LineReader) error {
    stop = 0
    distinctCheck := make(map[interface{}]bool)
    rowsChecked := 0
    var match bool
    //initial scan to find line positions
    for {
        if stop == 1 { break }
        rowsChecked++
        if rowsChecked % 10000 == 0 { messager <- "Scanning line "+Itoa(rowsChecked) }
        fromRow,err := reader.Read()
        if err != nil {break}
        match,err = evalWhere(q, &fromRow)
        if err != nil {return err}
        if match { reader.SavePos(q.sortCol) }
    }

    //sort matching line positions
    messager <- "Sorting Rows..."
    colType := q.files["file1"].types[q.sortCol]
    sort.Slice(reader.ValPositions, func(i, j int) bool {
        if reader.ValPositions[i].Val == nil && reader.ValPositions[j].Val == nil { return false
        } else if reader.ValPositions[i].Val == nil { return false
        } else if reader.ValPositions[j].Val == nil { return true
        } else {
            ret := false
            switch colType {
                case T_NULL:   fallthrough
                case T_STRING: ret = reader.ValPositions[i].Val.(string)        > reader.ValPositions[j].Val.(string)
                case T_INT:    ret = reader.ValPositions[i].Val.(int)           > reader.ValPositions[j].Val.(int)
                case T_FLOAT:  ret = reader.ValPositions[i].Val.(float64)       > reader.ValPositions[j].Val.(float64)
                case T_DATE:   ret = reader.ValPositions[i].Val.(time.Time).After(reader.ValPositions[j].Val.(time.Time))
            }
            if q.sortWay == 2 { return !ret }
            return ret
        }
        return false
    })

    //go back and retrieve lines in the right order
    reader.PrepareReRead()
    for i := range reader.ValPositions {
        if stop == 1 { stop = 0; messager <- "query cancelled"; break }
        fromRow,err := reader.ReadAt(i)
        if err != nil { break }
        if evalDistinct(q, &fromRow, distinctCheck) {
            execSelect(q, res, &fromRow)
            res.Numrows++;
            if res.Numrows >= reader.Limit { break }
            if res.Numrows % 1000 == 0 { messager <- "Retrieving line "+Itoa(res.Numrows) }
        }
    }
    return nil
}
