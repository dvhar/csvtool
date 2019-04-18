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
  "regexp"
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

    //pre-parse tokens and do stuff that only needs to be done once
    err := preParseTokens(q)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    if q.Save { saver <- saveData{Type : CH_HEADER, Header : q.ColSpec.NewNames}; <-savedLine }

    //prepare output
    res:= SingleQueryResult{
        Colnames : q.ColSpec.NewNames,
        Numcols: q.ColSpec.NewWidth,
        Types: q.ColSpec.NewTypes,
        Pos: q.ColSpec.NewPos,
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

        //recursive descent parser finds matches and retrieves results
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

//recursive descent parser for evaluating each row
func evalQuery(q *QuerySpecs, res *SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}, distinctCheck map[interface{}]bool) (bool,error) {

    //see if row matches condition
    match, err := evalWhere(q, fromRow)
    if err != nil || !match { return false, err }

    //see if row is distict if required
    match, err = evalDistinct(q, res, fromRow, distinctCheck)
    if err != nil || !match { return false, err }

    //copy entire row if selecting all
    if q.SelectAll  {
        if !q.MemFull && ( q.NeedAllRows || q.QuantityRetrieved <= 1000 ) {
            res.Vals = append(res.Vals, *fromRow)
            q.QuantityRetrieved++
        }
        if q.Save { saver <- saveData{Type : CH_ROW, Row : fromRow} ; <-savedLine }
        return true, err
    }

    //select columns if doing that
    q.BReset()
    for ;q.BTok().Id != BT_SCOL || q.BTok().Id == EOS; { q.BNext() }
    if q.BTok().Id == EOS { return false, errors.New("No columns selected") }
    countSelected := evalSelectCol(q, res, fromRow, selected, 0)
    if countSelected != q.ColSpec.NewWidth { return false, errors.New("returned "+Itoa(countSelected)+" columns. should be "+Itoa(q.ColSpec.NewWidth)) }
    return true, err
}

//see if there is a where token
func evalWhere(q *QuerySpecs, fromRow *[]interface{}) (bool, error) {
    for ; q.BTok().Id != KW_WHERE && q.BTok().Id != EOS; { q.BNext() }
    if q.BTok().Id == KW_WHERE { q.BNext(); return evalMultiComparison(q,fromRow) }
    return true, nil
}

//if there is a where token, evaluate match
func evalMultiComparison(q *QuerySpecs, fromRow*[]interface{}) (bool, error) {
    match := false
    negate := false
    var err error
    tok := q.BTok()

    //if found a negater
    if tok.Id == SP_NEGATE {
        negate = true
        tok = q.BNext()
    }

    //if found a column
    if tok.Id == BT_WCOL {
        match, err = evalComparison(q, fromRow)
        if err != nil { return false, err }
        if negate { match = !match }
    //if ( found instead of column
    } else if tok.Id == SP_LPAREN {
        q.BNext()
        match, err = evalMultiComparison(q, fromRow)
        if err != nil { return false, err }
        if negate { match = !match }
        //eat closing paren, return if this comparison is done
        q.BNext()
        if q.BPeek().Id == EOS || q.BPeek().Id == SP_RPAREN || (q.BPeek().Id & BT_AFTWR)!=0 {
            return match, err
        }
    }

    //if logical operator, perform logical operation with next comparision result
    if (q.BPeek().Id & LOGOP)!=0  {
        logop := q.BNext().Id
        q.BNext()
        nextExpr, err := evalMultiComparison(q, fromRow)
        if err != nil { return false, err }
        switch logop {
            case KW_AND: match = match && nextExpr
            case KW_OR:  match = match || nextExpr
        }
    }
    return match, err
}

//run each individual comparison
func evalComparison(q *QuerySpecs, fromRow *[]interface{}) (bool,error) {
    match := false
    negate := 0
    compCol := q.BTok()
    //flip result if 'not' or '!' in front of relop
    if q.BPeek().Id == SP_NEGATE {
        negate ^= 1
        q.BNext()
    }
    relop := q.BNext()
    compVal := q.BNext()
    if (relop.Id & RELOP) == 0  { return false, errors.New("Bad relational operator. Valid ones are =, !=, <>, >, >=, <, <=") }
    if compVal.Id != BT_WCOMP { return false, errors.New("Expected comparision value but got "+Sprint(compVal.Val)) }

    //if neither comparison value or column are null
    if compVal.Val != nil && (*fromRow)[compCol.Val.(int)] != nil {
        switch relop.Id {
            case KW_LIKE:  match = compVal.Val.(*regexp.Regexp).MatchString(Sprint((*fromRow)[compCol.Val.(int)]))
            case SP_NOEQ: negate ^= 1
                       fallthrough
            case SP_EQ :
                switch compVal.Dtype {
                    case T_DATE:   match = compVal.Val.(time.Time).Equal((*fromRow)[compCol.Val.(int)].(time.Time))
                    default:       match = compVal.Val == (*fromRow)[compCol.Val.(int)]
                }
            case SP_LESSEQ: negate ^= 1
                       fallthrough
            case SP_GREAT :
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = (*fromRow)[compCol.Val.(int)].(string) > compVal.Val.(string)
                    case T_INT:    match = (*fromRow)[compCol.Val.(int)].(int) > compVal.Val.(int)
                    case T_FLOAT:  match = (*fromRow)[compCol.Val.(int)].(float64) > compVal.Val.(float64)
                    case T_DATE:   match = (*fromRow)[compCol.Val.(int)].(time.Time).After(compVal.Val.(time.Time))
                }
            case SP_GREATEQ : negate ^= 1
                       fallthrough
            case SP_LESS:
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = (*fromRow)[compCol.Val.(int)].(string) < compVal.Val.(string)
                    case T_INT:    match = (*fromRow)[compCol.Val.(int)].(int) < compVal.Val.(int)
                    case T_FLOAT:  match = (*fromRow)[compCol.Val.(int)].(float64) < compVal.Val.(float64)
                    case T_DATE:   match = (*fromRow)[compCol.Val.(int)].(time.Time).Before(compVal.Val.(time.Time))
                }
        }

    //if comparison value is null
    } else if compVal.Val == nil {
        switch relop.Id {
            case SP_NOEQ: negate ^= 1
                       fallthrough
            case SP_EQ : match = (*fromRow)[compCol.Val.(int)] == nil
            default  : return false, errors.New("Invalid operation with null: "+relop.Val.(string)+". Valid operators: = != <>")
        }
    //if only column is null
    } else if compVal.Val != nil && (*fromRow)[compCol.Val.(int)] == nil  {
        switch relop.Id {
            case SP_NOEQ: negate ^= 1
                       fallthrough
            default: match = false
        }
    }
    //Println(relop,negate,match,compVal,(*fromRow)[compCol.Val.(int)])
    if negate==1 { match = !match }
    return match, nil

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
            if !q.MemFull && ( q.NeedAllRows || q.QuantityRetrieved <= 1000 ) {
                res.Vals = append(res.Vals, *selected)
                q.QuantityRetrieved++
            }
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
