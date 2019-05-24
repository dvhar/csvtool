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
  "errors"
  "sort"
  "io"
  "bytes"
)

var m runtime.MemStats
var totalMem uint64
var stop int
var active bool

type LineReader struct {
    FromRow []interface{}
    Limit int
    Fp *os.File
    LinePositions []int64
    Pos int64
    LineBuffer bytes.Buffer
    Tee io.Reader
    Cread *csv.Reader
    LineBytes []byte
}
func (l*LineReader) Init(q *QuerySpecs) {
    l.Fp,_ = os.Open(q.Fname)
    l.LinePositions = make([]int64,0)
    l.Tee = io.TeeReader(l.Fp, &l.LineBuffer)
    l.Cread = csv.NewReader(l.Tee)
    l.FromRow = make([]interface{}, q.ColSpec.Width)
    if q.QuantityLimit == 0 { l.Limit = 1<<62 } else { l.Limit = q.QuantityLimit }
}
func (l*LineReader) Read(q *QuerySpecs) ([]interface{},error) {
    line,err := l.Cread.Read()
    l.LineBytes,_ = l.LineBuffer.ReadBytes('\n')
    l.Pos += int64(len(l.LineBytes))
    l.LinePositions = append(l.LinePositions, l.Pos)
    for i,cell := range line {
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
    return l.FromRow,err
}

//run csv query
func csvQuery(q *QuerySpecs) (SingleQueryResult, error) {

    var err error
    //parse and do stuff that only needs to be done once
    q.Tree,err = parseQuery(q)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    if q.Save { saver <- saveData{Type : CH_HEADER, Header : q.ColSpec.NewNames}; <-savedLine }
    q.showLimit = 25000 / len(q.ColSpec.NewNames)

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
    var toRow []interface{}
    distinctCheck := make(map[interface{}]bool)
    active = true
    defer func(){
        active = false
        if q.Save { saver <- saveData{Type : CH_NEXT} }
    }()

    //prepare random access reader - not yet needed
    var lread LineReader
    lread.Init(q)

    //run query
    rowsChecked := 0
    stop = 0
    for ;res.Numrows<lread.Limit; {

        //watch out for memory ceiling
        runtime.ReadMemStats(&m)
        if m.Alloc > totalMem/3 { q.MemFull = true; if !q.Save { break } }

        //see if user wants to cancel
        if stop == 1 { stop = 0; messager <- "query cancelled"; break }

        //read line from csv file and allocate array for it
        fromRow,err := lread.Read(q)
        if err != nil {break}

        //find matches and retrieve results
        match, err := evalQuery(q, &res, &fromRow, &toRow, distinctCheck)
        if err != nil{ Println("evalQuery error in csvQuery:",err); return SingleQueryResult{}, err }
        if match {
            res.Numrows++
            lread.LinePositions = append(lread.LinePositions, lread.Pos)
        }

        //periodic updates
        rowsChecked++
        if rowsChecked % 10000 == 0 {
            messager <- "Scanning line "+Itoa(rowsChecked)+", "+Itoa(res.Numrows)+" matches so far"
        }
    }
    if err != nil { Println(err); return SingleQueryResult{}, err }
    err = evalOrderBy(q, &res)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    return res, nil
}

//check and retrieve matches
func evalQuery(q *QuerySpecs, res *SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}, distinctCheck map[interface{}]bool) (bool,error) {

    //see if row matches condition
    match, err := execWhere(q, fromRow)
    if err != nil || !match { return false, err }

    //see if row is distict if required
    match, err = evalDistinct(q, res, fromRow, distinctCheck)
    if err != nil || !match { return false, err }

    //retrieve columns
    execSelect(q,res,fromRow,selected)
    return true, err
}

//see if row has distinct value if looking for one. make sure this is the last check before retrieving row
func evalDistinct(q *QuerySpecs, res *SingleQueryResult, fromRow *[]interface{}, distinctCheck map[interface{}]bool) (bool,error) {
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
func evalOrderBy(q *QuerySpecs, res*SingleQueryResult) error {
    if q.SortWay == 0 { return nil }
    colIndex,err := getColumnIdx(q.ColSpec.NewNames, q.ColSpec.Names[q.SortCol])
    if err != nil { return errors.New("Could not find index of sorting column") }
    colType := res.Types[colIndex]
    sort.Slice(res.Vals, func(i, j int) bool {
        if res.Vals[i][colIndex] == nil && res.Vals[j][colIndex] == nil { return false
        } else if res.Vals[i][colIndex] == nil { return false
        } else if res.Vals[j][colIndex] == nil { return true
        } else {
            ret := false
            switch colType {
                case T_NULL:   fallthrough
                case T_STRING: ret = res.Vals[i][colIndex].(string) > res.Vals[j][colIndex].(string)
                case T_INT:    ret = res.Vals[i][colIndex].(int) > res.Vals[j][colIndex].(int)
                case T_FLOAT:  ret = res.Vals[i][colIndex].(float64) > res.Vals[j][colIndex].(float64)
                case T_DATE:   ret = res.Vals[i][colIndex].(time.Time).After(res.Vals[j][colIndex].(time.Time))
            }
            if q.SortWay == 2 { return !ret }
            return ret
        }
        return false
    })
    return nil
}
