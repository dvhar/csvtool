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


func (q *QuerySpecs) BNext() BToken {
    if q.BIdx < len(q.BTokArray)-1 {
        q.BIdx++
    } else { q.End = true }
    if q.End { return BToken{EOS, 0, 0} }
    return q.BTokArray[q.BIdx]
}
func (q QuerySpecs) BPeek() BToken {
    if q.BIdx < len(q.BTokArray)-1 {
        return q.BTokArray[q.BIdx+1]
    } else {
        return BToken{EOS, 0, 0}
    }
}
func (q QuerySpecs) BTok() BToken {
    if q.End || len(q.BTokArray)<1 { return BToken{EOS, 0, 0} }
    return q.BTokArray[q.BIdx]
}
func (q *QuerySpecs) BReset() { q.BIdx = 0; q.End = false }


var m runtime.MemStats
var totalMem uint64
var stop int
var active bool

//run csv query
func csvQuery(q *QuerySpecs) (SingleQueryResult, error) {

    var err error
    //pre-parse tokens and do stuff that only needs to be done once
    q.Tree,err = preParseTokens(q)
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
    var fromRow []interface{}
    var limit int
    distinctCheck := make(map[interface{}]bool)
    if q.QuantityLimit == 0 { limit = 1<<62 } else { limit = q.QuantityLimit }
    active = true
    defer func(){
        active = false
        if q.Save { saver <- saveData{Type : CH_NEXT} }
    }()

    //prepare random access reader - not yet needed
    fp,err := os.Open(q.Fname)
    linePositions := make([]int64,0)
    var lineBuffer bytes.Buffer
    var pos int64
    tee := io.TeeReader(fp, &lineBuffer)
    cread := csv.NewReader(tee)
    cread.Read()
    lineBytes,_ := lineBuffer.ReadBytes('\n')
    pos += int64(len(lineBytes))
    linePositions = append(linePositions, pos)


    //run query
    rowsChecked := 0
    stop = 0
    for ;res.Numrows<limit; {

        //watch out for memory ceiling
        runtime.ReadMemStats(&m)
        if m.Alloc > totalMem/3 {
            q.MemFull = true
            if !q.Save { break }
        }

        //see if user wants to cancel
        if stop == 1 {
            stop = 0
            messager <- "query cancelled"
            break
        }

        //read line from csv file and allocate array for it
        line, err := cread.Read()
        if err != nil {break}
        fromRow = make([]interface{}, q.ColSpec.Width)

        //calculate line position
        lineBytes,_ = lineBuffer.ReadBytes('\n')
        pos += int64(len(lineBytes))

        //read each cell from line
        for i,cell := range line {
            cell = s.TrimSpace(cell)
            if s.ToLower(cell) == "null" || cell == "" { fromRow[i] = nil
            } else {
                switch q.ColSpec.Types[i] {
                    case T_INT:    fromRow[i],_ = Atoi(cell)
                    case T_FLOAT:  fromRow[i],_ = ParseFloat(cell,64)
                    case T_DATE:   fromRow[i],_ = d.ParseAny(cell)
                    case T_NULL:   fallthrough
                    case T_STRING: fromRow[i] = cell
                }
            }
        }

        //find matches and retrieve results
        match, err := evalQuery(q, &res, &fromRow, &toRow, distinctCheck)
        if err != nil{ Println("evalQuery error in csvQuery:",err); return SingleQueryResult{}, err }
        if match {
            res.Numrows++
            linePositions = append(linePositions, pos)
        }
        q.BReset()

        //periodic updates
        rowsChecked++
        if rowsChecked % 10000 == 0 {
            messager <- "Scanning line "+Itoa(rowsChecked)+", "+Itoa(res.Numrows)+" matches so far"
        }
    }
    if err != nil { Println(err); return SingleQueryResult{}, err }
    err = evalOrderBy(q, &res)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    messager <- "Finishing a query..."
    return res, nil
}

//print parse tree for debuggging
func treePrint(n *Node, i int){
    if n==nil {return}
    for j:=0;j<i;j++ { Print("  ") }
    Println(enumMap[n.label+1000])
    for j:=0;j<i;j++ { Print("  ") }
    Println(n.tok1)
    treePrint(n.node1,i+1)
    treePrint(n.node2,i+1)
    treePrint(n.node3,i+1)
    treePrint(n.node4,i+1)
}

//recursive descent parser for evaluating each row
func evalQuery(q *QuerySpecs, res *SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}, distinctCheck map[interface{}]bool) (bool,error) {

    //see if row matches condition
    match, err := execWhere(q, fromRow)
    if err != nil || !match { return false, err }

    //see if row is distict if required
    match, err = evalDistinct(q, res, fromRow, distinctCheck)
    if err != nil || !match { return false, err }

    //copy entire row if selecting all
    treePrint(q.Tree.node1,0)
    execSelect(q, res, fromRow, selected)
    return true, err
}

func execSelect(q *QuerySpecs, res*SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}) {
    //select all if soing that
    if q.SelectAll  {
        if !q.MemFull && ( q.NeedAllRows || q.QuantityRetrieved <= q.showLimit ) {
            res.Vals = append(res.Vals, *fromRow)
            q.QuantityRetrieved++
        }
        if q.Save { saver <- saveData{Type : CH_ROW, Row : fromRow} ; <-savedLine }
    }
    //otherwise retrieve the selected columns
    execSelections(q,q.Tree.node1,res,fromRow,selected)
}
func execSelections(q *QuerySpecs, n *Node, res*SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}) {
    if n.tok1 == nil {
        if !q.MemFull && ( q.NeedAllRows || q.QuantityRetrieved <= q.showLimit ) {
            res.Vals = append(res.Vals, *selected)
            q.QuantityRetrieved++
        }
        if q.Save { saver <- saveData{Type : CH_ROW, Row : selected} ; <-savedLine}
        return
    }
    if n.label == N_SELECT { *selected = make([]interface{},0) }
    if n.label == N_SELECTIONS { *selected = append(*selected, (*fromRow)[n.tok1.(BToken).Val.(int)]) }
    execSelections(q,n.node1,res,fromRow,selected)
}


//add selected columns to results
func evalSelectCol(q *QuerySpecs, res*SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}, count int) int {
    tok := q.BTok()
    if tok.Id != BT_SCOL { return count }

    //add col to selected array
    if count < q.ColSpec.NewWidth {
        if count == 0 { *selected = make([]interface{}, q.ColSpec.NewWidth) }
        (*selected)[count] = (*fromRow)[tok.Val.(int)]
        if count == q.ColSpec.NewWidth - 1 {
            //all columns selected
            if !q.MemFull && ( q.NeedAllRows || q.QuantityRetrieved <= q.showLimit ) {
                res.Vals = append(res.Vals, *selected)
                q.QuantityRetrieved++
            }
            if q.Save { saver <- saveData{Type : CH_ROW, Row : selected} ; <-savedLine}
        if q.Save { saver <- saveData{Type : CH_ROW, Row : selected} ; <-savedLine}
        }
    }
    q.BNext()
    return evalSelectCol(q, res, fromRow, selected, count+1)
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
