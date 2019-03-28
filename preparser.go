package main
import (
  "encoding/csv"
  "os"
  "errors"
  s "strings"
  d "github.com/araddon/dateparse"
  . "strconv"
  "time"
)

//copied types from old version
type QuerySpecs struct {
    ColSpec Columns
    Fname string
    Qstring string
    ATokArray []AToken
    BTokArray []BToken
    AIdx int
    BIdx int
    Quantity int
    DistinctIdx int
    SelectAll bool
    SortCol int
    SortWay int
    Save bool
    MemFull bool
    End bool
}
func (q *QuerySpecs) ANext() *AToken {
    if q.AIdx < len(q.ATokArray)-1 { q.AIdx++ }
    return &q.ATokArray[q.AIdx]
}
func (q QuerySpecs) APeek() *AToken {
    if q.AIdx < len(q.ATokArray)-1 {
        return &q.ATokArray[q.AIdx+1]
    } else {
        return &q.ATokArray[q.AIdx]
    }
}
func (q QuerySpecs) ATok() *AToken { return &q.ATokArray[q.AIdx] }
func (q *QuerySpecs) AReset() { q.AIdx = 0 }
type Columns struct {
    Names []string
    NewNames []string
    Types []int
    NewTypes []int
    Width int
    NewWidth int
    NewPos []int
}
const (
    T_NULL = iota
    T_INT = iota
    T_FLOAT = iota
    T_DATE = iota
    T_STRING = iota
    T_UNKNOWN = iota
)
type BToken struct {
    Id int
    Val interface{}
    Dtype int
}
func max(a int, b int) int {
    if a>b { return a }
    return b
}
func getColumnIdx(colNames []string, column string) (int, error) {
    for i,col := range colNames {
        if s.ToLower(col) == s.ToLower(column) {
            return i, nil
        }
    }
    return 0, errors.New("getColumnIdx: column " + column + " not found")
}
//end of copy types

//get column types from file name
func inferTypes(q *QuerySpecs) error {

    //open file
    fp,err := os.Open(q.Fname)
    if err != nil { return errors.New("inferTypes: problem opening input file") }
    defer func(){ fp.Seek(0,0); fp.Close() }()

    cread := csv.NewReader(fp)
    line, err := cread.Read()
    if err != nil { return errors.New("inferTypes: problem reading input file") }
    //get col names and initialize blank types
    for i,entry := range line {
        q.ColSpec.Names = append(q.ColSpec.Names, entry)
        q.ColSpec.Types = append(q.ColSpec.Types, 0)
        q.ColSpec.Width = i+1
    }
    //get samples and infer types from them
    for j:=0;j<10000;j++ {
        line, err := cread.Read()
        if err != nil { break }
        for i,cell := range line {
            entry := s.TrimSpace(cell)
            if entry == "NULL" || entry == "null" || entry == "NA" || entry == "" {
              q.ColSpec.Types[i] = max(T_NULL, q.ColSpec.Types[i])
            } else if _, err := Atoi(entry); err == nil {
              q.ColSpec.Types[i] = max(T_INT, q.ColSpec.Types[i])
            } else if _, err := ParseFloat(entry,64); err == nil {
              q.ColSpec.Types[i] = max(T_FLOAT, q.ColSpec.Types[i])
            } else if _,err := d.ParseAny(entry); err == nil{
              q.ColSpec.Types[i] = max(T_DATE, q.ColSpec.Types[i])
            } else {
              q.ColSpec.Types[i] = max(T_STRING, q.ColSpec.Types[i])
            }
        }
    }
    println("got column data types")
    return  err
}

//fill out source csv ColSpecs
func evalFrom(q *QuerySpecs) error {
    //go straight to the from token or end
    for ; q.ATok().Id != KW_FROM && q.ATok().Id != EOS ; {q.ANext()}
    if q.ATok().Id == EOS && q.Fname == "" { return errors.New("No file to query") }
    if q.ATok().Id == EOS && q.Fname != "" { return inferTypes(q) }
    if q.ATok().Id == KW_FROM && q.APeek().Id != WORD {
        return errors.New("Unexpected token after 'from': "+q.APeek().Val) }
    if q.ATok().Id == KW_FROM && q.APeek().Id == WORD {
        q.Fname = q.APeek().Val
        err := inferTypes(q)
        q.AReset()
        return err
    }
    return errors.New("Unknown problem parsing 'from' file")
}

//top-level recursive descent pre-parser builds Token arrays and QuerySpecs
//saves main parser some time during query. currently only called once.
func preParseTokens(q* QuerySpecs) error {
    //first turn query string into A tokens
    err := tokenizeQspec(q)
    if err != nil { return err }

    //open file and get column info
    err = evalFrom(q)
    if err != nil { return err }

    //must start with select token. maybe add 'update' later
    if q.ATok().Id == KW_SELECT {
        q.ANext()
        return preTop(q)
    }
    return errors.New("query must start with select. found "+q.ATok().Val)
}
func preTop(q* QuerySpecs) error {
    var err error
    if q.ATok().Id == KW_TOP {
        q.Quantity, err = Atoi(q.APeek().Val)
        if err != nil { return errors.New("Expected number after 'top'. found "+q.APeek().Val) }
        q.ANext(); q.ANext()
    }
    err = preSelectCols(q)
    return err
}
func selectAll(q* QuerySpecs) {
    q.SelectAll = true
    q.ColSpec.NewNames = q.ColSpec.Names
    q.ColSpec.NewTypes = q.ColSpec.Types
    q.ColSpec.NewWidth = q.ColSpec.Width
    q.ColSpec.NewPos = make([]int,q.ColSpec.Width)
    for i,_ := range q.ColSpec.NewNames { q.ColSpec.NewPos[i] = i+1 }
}
func newCol(q* QuerySpecs,ii int) {
    if !q.SelectAll {
        q.ColSpec.NewNames = append(q.ColSpec.NewNames, q.ColSpec.Names[ii])
        q.ColSpec.NewTypes = append(q.ColSpec.NewTypes, q.ColSpec.Types[ii])
        q.ColSpec.NewPos = append(q.ColSpec.NewPos, ii+1)
        q.ColSpec.NewWidth++
    }
}
func preSelectCols(q* QuerySpecs) error {
    //eat commas
    for ;q.ATok().Id == SP_COMMA; { q.ANext() }
    //construct string from values between quotes
    if q.ATok().Id == SP_SQUOTE || q.ATok().Id == SP_DQUOTE {
        quote := q.ATok().Id
        var S string
        for ; q.ANext().Id != quote && q.ATok().Id != EOS; { S += q.ATok().Val }
        if q.ATok().Id == EOS { return errors.New("Quote was not terminated") }
        q.ANext()
        ii, err := getColumnIdx(q.ColSpec.Names, S)
        if err != nil { return errors.New("Column name not found: "+S) }
        q.BTokArray = append(q.BTokArray, BToken{BT_SCOL, ii, q.ColSpec.Types[ii]})
        newCol(q, ii)
        return preSelectCols(q)
    }
    //go to from zone
    if q.ATok().Id == KW_FROM || q.ATok().Id == KW_WHERE {
        if q.ColSpec.NewWidth == 0 { selectAll(q) }
        return preFrom(q)
    }
    //go past where zone
    if (q.ATok().Id & BT_AFTWR) != 0 {
        if q.ColSpec.NewWidth == 0 { selectAll(q) }
        return preAfterWhere(q)
    }
    //check for premature ending
    if q.ATok().Id == EOS && q.Fname == "" {
        return errors.New("Query ended before specifying a file")
    }
    //check for select all
    if q.ATok().Id == SP_ALL {
        selectAll(q)
        q.ANext()
        return preSelectCols(q)
    }
    //check for aggragate keywords
    if (q.ATok().Id & BT_AGG) != 0 {
        return preAggregates(q)
    }
    //check for invalid select columns
    if q.ATok().Id != WORD {
        return errors.New("Expected select column but found "+q.ATok().Val)
    }
    //parse selected column
    ii, err := parseColumnIndex(q)
    if err == nil {
        q.BTokArray = append(q.BTokArray, BToken{BT_SCOL, ii, q.ColSpec.Types[ii]})
        newCol(q, ii)
        q.ANext()
        return preSelectCols(q)
    }
    return err
}
func preAggregates(q* QuerySpecs) error {
    var err error
    if q.ATok().Id == KW_DISTINCT {
        if q.ANext().Id != WORD { return errors.New("Expected a column after 'distinct'. Got "+q.ATok().Val) }
        q.DistinctIdx, err = parseColumnIndex(q)
        if err != nil { return err }
    } else {
        return errors.New("Aggregate function not implemented: "+q.ATok().Val)
    }
    return preSelectCols(q)
}
func parseColumnIndex(q* QuerySpecs) (int,error) {
    c := q.ATok().Val
    if q.ATok().Id != WORD { return 0,errors.New("Expected column, got "+c) }
    ii, err := Atoi(c)
    if err == nil {
        if ii > q.ColSpec.Width { return 0,errors.New("Column number too big: "+c+". Max is "+Itoa(q.ColSpec.Width)) }
        if ii < 1 { return 0,errors.New("Column number too small: "+c) }
        return ii-1, nil
    }
    ii, err = getColumnIdx(q.ColSpec.Names, c)
    if err == nil {
        return ii, nil
    } else { return 0,errors.New("Column name not found: "+c) }
}
func preFrom(q* QuerySpecs) error {
    //go past where zone
    if (q.ATok().Id & BT_AFTWR) != 0 {
        return preAfterWhere(q)
    }
    //skip from - already got that
    if q.ATok().Id == KW_FROM {
        q.ANext(); q.ANext()
        return preFrom(q)
    }
    //if there is no where
    if q.ATok().Id == EOS { return nil }
    //if found a where token
    if q.ATok().Id == KW_WHERE {
        q.BTokArray = append(q.BTokArray, BToken{q.ATok().Id, q.ATok().Val, 0})
        q.ANext()
        return preWhere(q)
    }
    return errors.New("Unexpected token in 'from' section: "+q.ATok().Val)
}

var lastType int
func preWhere(q* QuerySpecs) error {
    tok := q.ATok()
    //if token that only appears after where section
    if tok.Id == EOS { return nil }
    if (tok.Id & BT_AFTWR) != 0 {
        return preAfterWhere(q)
    }
    //if found a word, it must be column
    if tok.Id == WORD {
        //TODO: between
        if q.APeek().Id == KW_BETWEEN { return preBetween(q) }

        ii, err := parseColumnIndex(q)
        if err == nil {
            q.BTokArray = append(q.BTokArray, BToken{BT_WCOL, ii, q.ColSpec.Types[ii]})
            lastType = q.ColSpec.Types[ii]
            q.ANext()
            return preWhere(q)
        } else { return err }
    }
    //if found a relop, add it to array and call WCOMP parser
    if (tok.Id & RELOP) != 0 {
        q.BTokArray = append(q.BTokArray, BToken{tok.Id, tok.Val, 0})
        q.ANext()
        tok,err := tokFromQuotes(q)
        if err != nil { return err }
        q.BTokArray = append(q.BTokArray, tok)
        return preWhere(q)
    }
    //parentheses, logops, negater
    if tok.Id == SP_LPAREN || tok.Id == SP_RPAREN || (tok.Id & LOGOP)!=0 || tok.Id == SP_NEGATE {
        q.BTokArray = append(q.BTokArray, BToken{tok.Id, tok.Val, 0})
        q.ANext()
        return preWhere(q)
    }
    return errors.New("Unexpected token in the where section: "+tok.Val)
}
//turn between clause into 2 comparisons with parenthese
func preBetween(q* QuerySpecs) error {
    var columnVal, val1, val2, relop1, relop2 BToken
    var firstSmaller bool

    //get comparison column and type
    ii, err := parseColumnIndex(q)
    if err != nil { return err }
    columnVal = BToken{BT_WCOL, ii, q.ColSpec.Types[ii]}
    lastType = q.ColSpec.Types[ii]

    //eat between token and get comparison values
    q.ANext()
    q.ANext()
    val1, err = tokFromQuotes(q)
    if err != nil { return err }
    if q.ATok().Id != KW_AND { return errors.New("Expected 'and' in between clause, got "+q.ATok().Val) }
    q.ANext()
    val2, err = tokFromQuotes(q)
    if err != nil { return err }

    //see which is smaller
    if val1.Dtype == T_NULL || val2.Dtype == T_NULL { return errors.New("Cannot use 'null' in between clause") }
    switch val1.Dtype {
        case T_INT:    firstSmaller = val1.Val.(int) < val2.Val.(int)
        case T_FLOAT:  firstSmaller = val1.Val.(float64) < val2.Val.(float64)
        case T_DATE:   firstSmaller = val1.Val.(time.Time).Before(val2.Val.(time.Time))
        case T_STRING: firstSmaller = val1.Val.(string) < val2.Val.(string)
    }
    if firstSmaller {
        relop1 = BToken{SP_GREATEQ, ">=", 0}
        relop2 = BToken{SP_LESS, "<", 0}
    } else {
        relop1 = BToken{SP_LESS, "<", 0}
        relop2 = BToken{SP_GREATEQ, ">=", 0}
    }

    //add tokens to B array
    q.BTokArray = append(q.BTokArray, BToken{SP_LPAREN, "(", 0})
    q.BTokArray = append(q.BTokArray, columnVal)
    q.BTokArray = append(q.BTokArray, relop1)
    q.BTokArray = append(q.BTokArray, val1)
    q.BTokArray = append(q.BTokArray, BToken{KW_AND, "and", 0})
    q.BTokArray = append(q.BTokArray, columnVal)
    q.BTokArray = append(q.BTokArray, relop2)
    q.BTokArray = append(q.BTokArray, val2)
    q.BTokArray = append(q.BTokArray, BToken{SP_RPAREN, ")", 0})

    return preWhere(q)
}
//comparison values in where section
func tokFromQuotes(q* QuerySpecs) (BToken,error) {
    var good bool
    var tok BToken
    var err error
    //add to array if just a word
    if q.ATok().Id == WORD {
        tok = BToken{BT_WCOMP, q.ATok().Val, lastType}
        good = true
    }
    //construct string from values between quotes
    if q.ATok().Id == SP_SQUOTE || q.ATok().Id == SP_DQUOTE {
        quote := q.ATok().Id
        var S string
        for ; q.ANext().Id != quote && q.ATok().Id != EOS; { S += q.ATok().Val }
        if q.ATok().Id == EOS { return tok, errors.New("Quote was not terminated") }
        tok = BToken{BT_WCOMP, S, lastType}
        good = true
    }
    //give interface the right type and append token
    if good {
        if s.ToLower(tok.Val.(string)) == "null" { tok.Dtype = T_NULL }
        switch tok.Dtype {
            case T_INT:    tok.Val,err = Atoi(tok.Val.(string))
            case T_FLOAT:  tok.Val,err = ParseFloat(tok.Val.(string), 64)
            case T_DATE:   tok.Val,err = d.ParseAny(tok.Val.(string))
            case T_NULL:   tok.Val = nil
            case T_STRING: tok.Val = tok.Val.(string)
        }
        q.ANext()
        return tok, err
    }
    return tok, errors.New("Expected a comparision value but got "+q.ATok().Val)
}
//modify this if adding 'group by' function. currently order is only thing after where
func preAfterWhere(q* QuerySpecs) error {
    if q.ATok().Id == EOS { return nil }
    if q.ATok().Id == KW_ORDER {
        if q.ANext().Id != KW_BY { return errors.New("Expected 'by' after 'order'. Found "+q.ATok().Val) }
        /*if*/ q.ANext()//.Id != WORD { return errors.New("Expected column after 'order by'. Found "+q.ATok().Val) }
        ii, err := parseColumnIndex(q)
        if err == nil {
            q.SortCol = ii
            q.SortWay = 1
            q.ANext()
            preOrderMethod(q)
        } else { return err }
    }
    return nil
}
func preOrderMethod(q* QuerySpecs) error {
    if q.ATok().Id == EOS { return nil }
    if q.ATok().Id == KW_ORDHOW { q.SortWay = 2 }
    return preAfterWhere(q)
}
