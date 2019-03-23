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

//run csv query
func csvQuery(q *QuerySpecs) (SingleQueryResult, error) {
    //turn string into A tokens
    err := tokenizeQspec(q)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    //do stuff that only needs to be done once and create B tokens for faster parsing
    err = preParseTokens(q)
    if err != nil { Println(err); return SingleQueryResult{}, err }
	saver <- chanData{Type : CH_HEADER, Header : q.ColSpec.NewNames}

    //prepare input and output
    totalMem = memory.TotalMemory()
    fp,err := os.Open(q.Fname)
    cread := csv.NewReader(fp)
    res:= SingleQueryResult{
        Colnames : q.ColSpec.NewNames,
        Numcols: q.ColSpec.NewWidth,
        Types: q.ColSpec.NewTypes,
    }
    var toRow []interface{}
    var fromRow []interface{}
    var limit int
    if q.Quantity == 0 { limit = 1E9 } else { limit = q.Quantity }

    //run the query
    cread.Read()
    rowsChecked := 0
    for j:=0;j<limit; {
        //read line from csv file and allocate array for it
        line, err := cread.Read()
        if err != nil {break}
        fromRow = make([]interface{}, q.ColSpec.Width)
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
        match, err := evalQuery(q, &res, &fromRow, &toRow)
        if err != nil{ Println("evalQuery error in csvQuery:",err); return SingleQueryResult{}, err }
        if match { j++; res.Numrows++ }
        q.BReset()

        //watch out for memory ceiling
        runtime.ReadMemStats(&m)
        if m.Alloc > totalMem/3 {
            println("reached soft memory limit")
            messager <- "Not enough memory for all results"
            break
        }

        //periodic updates
        rowsChecked++
        if rowsChecked % 10000 == 0 {
            messager <- "Scanning line "+Itoa(rowsChecked)+", "+Itoa(j)+" matches so far"
        }
    }
    err = evalOrderBy(q, &res)
    if err != nil { Println(err); return SingleQueryResult{}, err }
    messager <- "Finishing a query..."
    if q.Save { saver <- chanData{Type : CH_NEXT} }
    return res, nil
}

//recursive descent parser for evaluating each row
func evalQuery(q *QuerySpecs, res *SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}) (bool,error) {

    //see if row matches expression
    match, err := evalWhere(q, fromRow)
    if err != nil{ return false, err }
    if !match { return false, nil }

    //see if row is distict if required
    match, err = evalDistinct(q, res, fromRow)
    if !match { return false, nil }

    //copy entire row if selecting all
    if q.SelectAll {
        res.Vals = append(res.Vals, *fromRow)
        if q.Save { saver <- chanData{Type : CH_ROW, Row : fromRow} }
        return true, nil
    }

    //select columns if doing that
    q.BReset()
    for ;q.BPeek().Id != BT_SCOL || q.BPeek().Id == EOS; { q.BNext() }
    if q.BPeek().Id == EOS { return false, errors.New("No columns selected") }
    countSelected := evalSelectCol(q, res, fromRow, selected, 0)
    if countSelected != q.ColSpec.NewWidth { return true, errors.New("returned "+Itoa(countSelected)+" columns. should be "+Itoa(q.ColSpec.NewWidth)) }
    return true, nil
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
        q.BNext()
    }

    //if found a column
    if tok.Id == BT_WCOL {
        match, err = evalComparison(q, fromRow)
        if err != nil { return false, err }
    //if ( found instead of column
    } else if tok.Id == SP_LPAREN {
        q.BNext()
        match, err = evalMultiComparison(q, fromRow)
        //eat closing paren, return if this expression is done
        q.BNext()
        if q.BPeek().Id == EOS || q.BPeek().Id == SP_RPAREN || (q.BPeek().Id & BT_AFTWR)!=0 {
            if negate { match = !match }
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
    if negate { match = !match }
    return match, nil
}
//run each individual comparison
func evalComparison(q *QuerySpecs, fromRow *[]interface{}) (bool,error) {
    match := false
    negate := 0
    compCol := q.BTok()
    if q.BPeek().Id == SP_NEGATE {
        negate ^= 1
        q.BNext()
    }
    relop := q.BNext()
    compVal := q.BNext()

    if compVal.Val != nil && (*fromRow)[compCol.Val.(int)] != nil {
        switch relop.Val.(string) {
            case "<>": negate ^= 1
                       fallthrough
            case "=" :
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = compVal.Val.(string) == (*fromRow)[compCol.Val.(int)].(string)
                    case T_INT:    match = compVal.Val.(int) == (*fromRow)[compCol.Val.(int)].(int)
                    case T_FLOAT:  match = compVal.Val.(float64) == (*fromRow)[compCol.Val.(int)].(float64)
                    case T_DATE:   match = compVal.Val.(time.Time).Equal((*fromRow)[compCol.Val.(int)].(time.Time))
                }
            case "<=": negate ^= 1
                       fallthrough
            case ">" :
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = (*fromRow)[compCol.Val.(int)].(string) > compVal.Val.(string)
                    case T_INT:    match = (*fromRow)[compCol.Val.(int)].(int) > compVal.Val.(int)
                    case T_FLOAT:  match = (*fromRow)[compCol.Val.(int)].(float64) > compVal.Val.(float64)
                    case T_DATE:   match = (*fromRow)[compCol.Val.(int)].(time.Time).After(compVal.Val.(time.Time))
                }
            case ">=" : negate ^= 1
                       fallthrough
            case "<":
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = (*fromRow)[compCol.Val.(int)].(string) < compVal.Val.(string)
                    case T_INT:    match = (*fromRow)[compCol.Val.(int)].(int) < compVal.Val.(int)
                    case T_FLOAT:  match = (*fromRow)[compCol.Val.(int)].(float64) < compVal.Val.(float64)
                    case T_DATE:   match = (*fromRow)[compCol.Val.(int)].(time.Time).Before(compVal.Val.(time.Time))
                }
        }

    //if comparing to null
    } else if compVal.Val == nil {
        switch relop.Val.(string) {
            case "<>": negate ^= 1
                       fallthrough
            case "=" : match = (*fromRow)[compCol.Val.(int)] == nil
            default  : return false, errors.New("Invalid operation with null: "+relop.Val.(string)+". Valid operators: = != <>")
        }
    }
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
            res.Vals = append(res.Vals, *selected)
            if q.Save { saver <- chanData{Type : CH_ROW, Row : selected} }
        }
    }
    q.BNext()
    return evalSelectCol(q, res, fromRow, selected, count+1)
}
//see if row has distinct value if looking for one
func evalDistinct(q *QuerySpecs, res *SingleQueryResult, fromRow *[]interface{}) (bool,error) {
    if q.DistinctIdx < 0 { return true, nil }
    var match bool
    compVal := (*fromRow)[q.DistinctIdx]
    colType := q.ColSpec.NewTypes[q.DistinctBackcheck]
    for _,entry := range res.Vals {
        switch colType {
            case T_NULL:   fallthrough
            case T_STRING: match = compVal.(string) == entry[q.DistinctBackcheck].(string)
            case T_INT:    match = compVal.(int) == entry[q.DistinctBackcheck].(int)
            case T_FLOAT:  match = compVal.(float64) == entry[q.DistinctBackcheck].(float64)
            case T_DATE:   match = compVal.(time.Time).Equal(entry[q.DistinctBackcheck].(time.Time))
        }
        if match { return false, nil }
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
