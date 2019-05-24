package main
import (
  . "fmt"
  "github.com/pbnjay/memory"
  "encoding/csv"
  "os"
  "runtime"
  s "strings"
  d "github.com/araddon/dateparse"
  . "strconv"
  "time"
  "sort"
  "io"
  "bytes"
)

var m runtime.MemStats
var totalMem uint64
var stop int
var active bool

//line reader type and functions/methods
type ValPos struct {
    Pos int64
    Val interface{}
}
type LineReader struct {
    FromRow []interface{}
    Limit int
    MaxLineSize int
    Fp *os.File
    LinePositions []int64
    ValPositions []ValPos
    SavedValues []interface{}
    Pos int64
    PrevPos int64
    LineBuffer bytes.Buffer
    Tee io.Reader
    Cread *csv.Reader
    ByteReader *bytes.Reader
    LineBytes []byte
}
func (l*LineReader) SavePos(colNo int) {
    l.ValPositions = append(l.ValPositions, ValPos{l.PrevPos, l.FromRow[colNo]})
}
func (l*LineReader) InitReRead() {
    l.LineBytes = make([]byte, l.MaxLineSize)
    l.ByteReader = bytes.NewReader(l.LineBytes)
}
func (l*LineReader) Init(q *QuerySpecs) {
    l.Fp,_ = os.Open(q.Fname)
    l.ValPositions = make([]ValPos,0)
    l.SavedValues = make([]interface{},0)
    l.Tee = io.TeeReader(l.Fp, &l.LineBuffer)
    l.Cread = csv.NewReader(l.Tee)
    l.FromRow = make([]interface{}, q.ColSpec.Width)
    if q.QuantityLimit == 0 { l.Limit = 1<<62 } else { l.Limit = q.QuantityLimit }
}
func (l*LineReader) convertLine(inline *[]string, q *QuerySpecs) {
    for i,cell := range (*inline) {
        cell = s.TrimSpace(cell)
        if s.ToLower(cell) == "null" || cell == "" { l.FromRow[i] = nil
        } else {
            switch q.ColSpec.Types[i] {
                case T_INT:    l.FromRow[i],_ = Atoi(cell)
                case T_FLOAT:  l.FromRow[i],_ = ParseFloat(cell,64)
                case T_DATE:   l.FromRow[i],_ = d.ParseAny(cell)
                case T_NULL:   fallthrough
                case T_STRING: l.FromRow[i] = cell
            }
        }
    }
}
func (l*LineReader) Read(q *QuerySpecs) ([]interface{},error) {
    line, err := l.Cread.Read()
    l.LineBytes, _ = l.LineBuffer.ReadBytes('\n')
    size := len(l.LineBytes)
    if l.MaxLineSize < size { l.MaxLineSize = size }
    l.PrevPos = l.Pos
    l.Pos += int64(size)
    l.convertLine(&line, q)
    return l.FromRow, err
}
func (l*LineReader) ReadAt(q *QuerySpecs, linePos int64) ([]interface{},error) {
    l.Fp.ReadAt(l.LineBytes, linePos)
    l.ByteReader.Seek(0,0)
    l.Cread = csv.NewReader(l.ByteReader)
    line, err := l.Cread.Read()
    l.convertLine(&line, q)
    return l.FromRow, err
}

//run csv query
func csvQuery(q *QuerySpecs) (SingleQueryResult, error) {

    var err error
    //parse and do stuff that only needs to be done once
    q.Tree,err = parseQuery(q)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    if q.Save { saver <- saveData{Type : CH_HEADER, Header : q.ColSpec.NewNames}; <-savedLine }
    q.showLimit = 20000 / len(q.ColSpec.NewNames)

    //prepare output
    res:= SingleQueryResult{
        Colnames : q.ColSpec.NewNames,
        Numcols: q.ColSpec.NewWidth,
        Types: q.ColSpec.NewTypes,
        Pos: q.ColSpec.NewPos,
        ShowLimit : q.showLimit,
    }

    //prepare some other things
    totalMem = memory.TotalMemory()
    active = true
    defer func(){
        active = false
        if q.Save { saver <- saveData{Type : CH_NEXT} }
    }()

    //prepare reader
    var reader LineReader
    reader.Init(q)

    //run normal query
    if q.SortWay == 0 {
        err = normalQuery(q, &res, &reader)
        if err != nil { Println(err); return SingleQueryResult{}, err }
    } else {
        err = evalOrderBy(q, &res, &reader)
    }

    if err != nil { Println(err); return SingleQueryResult{}, err }
    return res, nil
}


//retrieve results on first pass
func normalQuery(q *QuerySpecs, res *SingleQueryResult, reader *LineReader) error {
    var err error
    rowsChecked := 0
    stop = 0
    distinctCheck := make(map[interface{}]bool)
    for ;res.Numrows<reader.Limit; {
        //determine of need to stop
        runtime.ReadMemStats(&m)
        if m.Alloc > 2.0*totalMem/3.0 { q.MemFull = true; if !q.Save { break } }
        if stop == 1 { stop = 0; messager <- "query cancelled"; break }

        //read line from csv file
        fromRow,err := reader.Read(q)
        if err != nil {break}

        //find matches and retrieve results
        match, err := evalMatch(q, &fromRow, distinctCheck)
        if err != nil{ Println("evalMatch error in csvQuery:",err); return err }
        if match {
            res.Numrows++;
            execSelect(q, res, &fromRow)
        }

        //periodic updates
        rowsChecked++
        if rowsChecked % 10000 == 0 { messager <- "Scanning line "+Itoa(rowsChecked)+", "+Itoa(res.Numrows)+" matches so far" }
    }
    return err
}

//check and retrieve matches
func evalMatch(q *QuerySpecs, fromRow *[]interface{}, distinctCheck map[interface{}]bool) (bool,error) {

    //see if row matches condition
    match, err := execWhere(q, fromRow)
    if err != nil || !match { return false, err }

    //see if row is distict if required
    match, err = evalDistinct(q, fromRow, distinctCheck)
    if err != nil || !match { return false, err }

    return true, err
}

//see if row has distinct value if looking for one. make sure this is the last check before retrieving row
func evalDistinct(q *QuerySpecs, fromRow *[]interface{}, distinctCheck map[interface{}]bool) (bool,error) {
    if q.DistinctIdx < 0 { return true, nil }
    compVal := (*fromRow)[q.DistinctIdx]
    //ok means not distinct
    _,ok := distinctCheck[compVal]
    if ok {
        return false, nil
    } else {
        distinctCheck[compVal] = true
    }
    return true,nil
}

//sort results
func evalOrderBy(q *QuerySpecs, res *SingleQueryResult, reader *LineReader) error {
    stop = 0
    distinctCheck := make(map[interface{}]bool)
    //find line positions
    for {
        if stop == 1 { stop = 0; messager <- "query cancelled"; break }
        fromRow,err := reader.Read(q)
        if err != nil {break}
        match, err := evalMatch(q, &fromRow, distinctCheck)
        if err != nil{ return err }
        if match { reader.SavePos(q.SortCol); res.Numrows++ }
    }

    //sort matching line positions
    colType := q.ColSpec.Types[q.SortCol]
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
            if q.SortWay == 2 { return !ret }
            return ret
        }
        return false
    })

    //go back and retrieve lines in the right order
    reader.InitReRead()
    for i := 0; i < len(reader.ValPositions); i++ {
        fromRow,err := reader.ReadAt(q, reader.ValPositions[i].Pos)
        execSelect(q, res, &fromRow)
        if err != nil {return err}
    }
    return nil
}
