//file provides the preParseTokens function
package main
import (
  "regexp"
  "encoding/csv"
  "os"
  "errors"
  s "strings"
  d "github.com/araddon/dateparse"
  . "strconv"
  . "fmt"
  "time"
)

type QuerySpecs struct {
    ColSpec Columns
    Fname string
    Qstring string
    ATokArray []AToken
    BTokArray []BToken
    AIdx int
    BIdx int
    QuantityLimit int
    QuantityRetrieved int
    NeedAllRows bool
    DistinctIdx int
    SelectAll bool
    SortCol int
    SortWay int
    Save bool
    MemFull bool
    Like bool
    End bool
    ParseCol int
    showLimit int
    LastColumn BToken
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
const (
    N_PPTOKENS = iota
    N_SELECT = iota
    N_TOP = iota
    N_SELECTIONS = iota
    N_COLUMN = iota
    N_SPECIAL = iota
    N_FROM = iota
    N_WHERE = iota
    N_CONDITIONS = iota
    N_BETWEEN = iota
    N_MORE = iota
    N_COMPARE = iota
    N_REL = iota
    N_ORDER = iota
    N_ORDERM = iota
)
type Node struct {
    label int
    tok1 interface{}
    tok2 interface{}
    tok3 interface{}
    tok4 interface{}
    node1 *Node
    node2 *Node
    node3 *Node
    node4 *Node
}
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
    COL_APPEND = iota
    COL_GETIDX = iota
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
    return 0, errors.New("Column " + column + " not found")
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

//get column types from first 10000 rows
func inferTypes(q *QuerySpecs) error {

    //open file
    fp,err := os.Open(q.Fname)
    if err != nil { return errors.New("problem opening input file") }
    defer func(){ fp.Seek(0,0); fp.Close() }()

    cread := csv.NewReader(fp)
    line, err := cread.Read()
    if err != nil { return errors.New("problem reading input file") }
    //get col names and initialize blank types
    for i,entry := range line {
        q.ColSpec.Names = append(q.ColSpec.Names, entry)
        q.ColSpec.Types = append(q.ColSpec.Types, 0)
        q.ColSpec.Width = i+1
    }
    //regex catches string that would otherwise get typed as int
    LeadingZeroString := regexp.MustCompile(`^0\d+$`)

    //get samples and infer types from them
    for j:=0;j<10000;j++ {
        line, err := cread.Read()
        if err != nil { break }
        for i,cell := range line {
            entry := s.TrimSpace(cell)
            if s.ToLower(entry) == "null" || entry == "NA" || entry == "" {
              q.ColSpec.Types[i] = max(T_NULL, q.ColSpec.Types[i])
            } else if LeadingZeroString.MatchString(entry) {
              q.ColSpec.Types[i] = T_STRING
            } else if _, err := Atoi(entry); err == nil {
              q.ColSpec.Types[i] = max(T_INT, q.ColSpec.Types[i])
            } else if _, err := ParseFloat(entry,64); err == nil {
              q.ColSpec.Types[i] = max(T_FLOAT, q.ColSpec.Types[i])
            } else if _,err := d.ParseAny(entry); err == nil{
              q.ColSpec.Types[i] = max(T_DATE, q.ColSpec.Types[i])
            } else {
              q.ColSpec.Types[i] = T_STRING
            }
        }
    }
    println("got column data types")
    return  err
}

//open file and call type inferrer
func evalFrom(q *QuerySpecs) error {
    //go straight to the from token or end
    if q.ATok().Id != KW_SELECT { return errors.New("Query must start with select. found "+q.ATok().Val) }
    for ; q.ATok().Id != KW_FROM && q.ATok().Id != EOS ; {q.ANext()}
    if q.ATok().Id == EOS && q.Fname == "" { return errors.New("Could not find a valid 'from file' part of query") }
    if q.ATok().Id == EOS && q.Fname != "" { return inferTypes(q) }
    if q.ATok().Id == KW_FROM && q.APeek().Id != WORD {
        return errors.New("Unexpected token after 'from': "+q.APeek().Val) }
    if q.ATok().Id == KW_FROM && q.APeek().Id == WORD {
        q.Fname = q.APeek().Val
        err := inferTypes(q)
        q.AReset()
        return err
    }
    return errors.New("Unknown problem parsing 'from file' part of query")
}

//recursive descent pre-parser builds Token arrays and QuerySpecs
func preParseTokens(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_PPTOKENS}
    n.tok1 = q
    //first turn query string into A tokens
    err := tokenizeQspec(q)
    if err != nil { return n,err }

    //then open file and get column info
    err = evalFrom(q)
    if err != nil { return n,err }

    //select section
    n.node1,err =  preParseSelect(q)
    if err != nil { return n,err }

    //skip from section because already evaluated
    n.node2,err = preParseFrom(q)
    if err != nil { return n,err }

    //where section
    n.node3,err =  preParseWhere(q)
    if err != nil { return n,err }

    //Order by
    n.node4,err =  preParseOrder(q)
    return n,err
}

func preParseSelect(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_SELECT}
    var err error
    if q.ATok().Id != KW_SELECT { return n,errors.New("Expected 'select' token. found "+q.ATok().Val) }
    q.ANext()
    n.node1,err = preParseTop(q)
    if err != nil { return n,err }
    n.node2,err = preParseSelections(q)
    return n,err
}

func preParseTop(q* QuerySpecs) (*Node,error) {
    //terminal
    n := &Node{label:N_TOP}
    var err error
    if q.ATok().Id == KW_TOP {
        q.QuantityLimit, err = Atoi(q.APeek().Val)
        if err != nil { return n,errors.New("Expected number after 'top'. found "+q.APeek().Val) }
        q.ANext(); q.ANext()
    }
    return n,nil
}

func preParseSelections(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_SELECTIONS}
    var err error
    switch q.ATok().Id {
        case SP_ALL:
            selectAll(q)
            q.ANext()
            n.node1,err = preParseSelections(q)
            return n,err
        //non-column words in select section
        case KW_DISTINCT:
            n.node1,err = preParseSpecial(q)
            return n,err
        //column
        case WORD: fallthrough
        case SP_SQUOTE: fallthrough
        case SP_DQUOTE:
            q.ParseCol = COL_APPEND
            n.node1,err = preParseColumn(q)
            if err != nil { return n,err }
            n.node2,err = preParseSelections(q)
            return n,err
        case KW_FROM:
            if q.ColSpec.NewWidth == 0 { selectAll(q) }
    }
    return n,nil
}

func preParseColumn(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_COLUMN}
    var ii int
    var err error
    switch q.ATok().Id {
        //parse selected column
        case WORD:
            c := q.ATok().Val
            ii, err = Atoi(c)
            //if it's a number
            if err == nil {
                if ii > q.ColSpec.Width { return n,errors.New("Column number too big: "+c+". Max is "+Itoa(q.ColSpec.Width)) }
                if ii < 1 { return n,errors.New("Column number too small: "+c) }
                ii -= 1
            //if it's a name
            } else {
                ii, err = getColumnIdx(q.ColSpec.Names, c)
            }
        //parse column from quotes
        case SP_SQUOTE: fallthrough
        case SP_DQUOTE:
            quote := q.ATok().Id
            var S string
            for ; q.ANext().Id != quote && q.ATok().Id != EOS; { S += q.ATok().Val }
            if q.ATok().Id == EOS { return n,errors.New("Quote was not terminated") }
            ii, err = getColumnIdx(q.ColSpec.Names, S)
    }
    if err != nil { return n,err }
    //see if appending to token array or just getting index
    switch q.ParseCol {
        case COL_APPEND:
            q.BTokArray = append(q.BTokArray, BToken{BT_SCOL, ii, q.ColSpec.Types[ii]})
            newCol(q, ii)
        case COL_GETIDX:
            q.ParseCol = ii
    }
    q.ANext()
    return n,err
}

func preParseSpecial(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_SPECIAL}
    var err error
    switch q.ATok().Id {
        case KW_DISTINCT:
            q.ANext()
            q.ParseCol = COL_GETIDX
            n.node1,err = preParseColumn(q)
            if err != nil { return n,err }
            q.DistinctIdx = q.ParseCol
            if !q.SelectAll {
                q.BTokArray = append(q.BTokArray, BToken{BT_SCOL, q.ParseCol, q.ColSpec.Types[q.ParseCol]})
                newCol(q, q.ParseCol)
            }
            return n,err
    }
    return n,errors.New("Unexpected token in 'select' section:"+q.ATok().Val)
}

func preParseFrom(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_FROM}
    if q.ATok().Id != KW_FROM { return n,errors.New("Expected 'from'. Found: "+q.ATok().Val) }
    q.ANext()
    q.ANext()
    if q.ATok().Id == KW_AS {
        q.ANext()
        q.ANext()
    }
    return n,nil
}

func preParseWhere(q*QuerySpecs) (*Node,error) {
    n := &Node{label:N_WHERE}
    var err error
    if q.ATok().Id != KW_WHERE { return n,nil }
    q.BTokArray = append(q.BTokArray, BToken{q.ATok().Id, q.ATok().Val, 0})
    q.ANext()
    n.node1,err = preParseConditions(q)
    return n,err
}

func preParseConditions(q*QuerySpecs) (*Node,error) {
    n := &Node{label:N_CONDITIONS}
    var err error
    //negater before conditions
    if q.ATok().Id == SP_NEGATE {
        q.BTokArray = append(q.BTokArray, BToken{q.ATok().Id, q.ATok().Val, 0})
        q.ANext()
    }
    switch q.ATok().Id {
        case SP_LPAREN:
            tok := q.ATok()
            q.BTokArray = append(q.BTokArray, BToken{tok.Id, tok.Val, 0})
            q.ANext();
            n.node1,err = preParseConditions(q)
            if err != nil { return n,err }
            tok = q.ATok()
            if tok.Id != SP_RPAREN { return n,errors.New("No closing parentheses. Found: "+tok.Val) }
            q.BTokArray = append(q.BTokArray, BToken{tok.Id, tok.Val, 0})
            q.ANext()
            n.node2,err = preparseMore(q)
            return n,err
        case WORD: fallthrough
        case SP_DQUOTE: fallthrough
        case SP_SQUOTE:
            n.node1,err = preParseCompare(q)
            if err != nil { return n,err }
            n.node2,err = preparseMore(q)
            return n,err
    }
    return n,errors.New("Unexpected token in 'where' section: "+q.ATok().Val)
}

func preParseCompare(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_COMPARE}
    var err error
    q.ParseCol = COL_GETIDX
    n.node1,err = preParseColumn(q)
    if err != nil { return n,err }
    ii := q.ParseCol
    q.LastColumn = BToken{BT_WCOL, ii, q.ColSpec.Types[ii]}
    n.node2,err = preParseRel(q)
    return n,err
}

func preParseRel(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_REL}
    var err error
    //negater before relop
    if q.ATok().Id == SP_NEGATE {
        q.BTokArray = append(q.BTokArray, BToken{q.ATok().Id, q.ATok().Val, 0})
        q.ANext()
    }
    //between
    if q.ATok().Id == KW_BETWEEN {
        n.node1,err = preParseBetween(q)
        return n,err
    }
    //relop and value
    if (q.ATok().Id & RELOP) != 0 {
        q.BTokArray = append(q.BTokArray, q.LastColumn)
        tok := q.ATok()
        if tok.Id == KW_LIKE { q.Like = true; }
        q.BTokArray = append(q.BTokArray, BToken{tok.Id, tok.Val, 0})
        q.ANext()
        btok,err := tokFromQuotes(q)
        //if relop is 'like', compile a regex
        if q.Like {
            q.Like = false
            re := regexp.MustCompile("%")
            btok.Val = re.ReplaceAllString(Sprint(btok.Val), ".*")
            re = regexp.MustCompile("_")
            btok.Val = re.ReplaceAllString(Sprint(btok.Val), ".")
            btok.Val,err = regexp.Compile("(?i)^"+btok.Val.(string)+"$")
        }
        q.BTokArray = append(q.BTokArray, btok)
        return n,err
    }
    return n,errors.New("Expected relational operator. Found: "+q.ATok().Val)
}

func preparseMore(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_MORE}
    var err error
    if (q.ATok().Id & LOGOP) == 0 { return n,nil }
    q.BTokArray = append(q.BTokArray, BToken{q.ATok().Id, q.ATok().Val, 0})
    q.ANext()
    n.node1,err = preParseConditions(q)
    return n,err
}

//turn between clause into 2 comparisons with parenthese
func preParseBetween(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_BETWEEN}
    var  val1, val2, relop1, relop2 BToken
    var firstSmaller bool
    var err error

    //eat between token and get comparison values
    q.ANext()
    val1, err = tokFromQuotes(q)
    if err != nil { return n,err }
    if q.ATok().Id != KW_AND { return n,errors.New("Expected 'and' in between clause, got "+q.ATok().Val) }
    q.ANext()
    val2, err = tokFromQuotes(q)
    if err != nil { return n,err }

    //see which is smaller
    if val1.Dtype == T_NULL || val2.Dtype == T_NULL { return n,errors.New("Cannot use 'null' with 'between'") }
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
    q.BTokArray = append(q.BTokArray, q.LastColumn)
    q.BTokArray = append(q.BTokArray, relop1)
    q.BTokArray = append(q.BTokArray, val1)
    q.BTokArray = append(q.BTokArray, BToken{KW_AND, "and", 0})
    q.BTokArray = append(q.BTokArray, q.LastColumn)
    q.BTokArray = append(q.BTokArray, relop2)
    q.BTokArray = append(q.BTokArray, val2)
    q.BTokArray = append(q.BTokArray, BToken{SP_RPAREN, ")", 0})

    return n,err
}

//comparison values in where section
func tokFromQuotes(q* QuerySpecs) (BToken,error) {
    var good bool
    var tok BToken
    var err error
    //add to array if just a word
    if q.ATok().Id == WORD {
        tok = BToken{BT_WCOMP, q.ATok().Val, q.LastColumn.Dtype}
        good = true
    }
    //construct string from values between quotes
    if q.ATok().Id == SP_SQUOTE || q.ATok().Id == SP_DQUOTE {
        quote := q.ATok().Id
        var S string
        for ; q.ANext().Id != quote && q.ATok().Id != EOS; { S += q.ATok().Val }
        if q.ATok().Id == EOS { return tok, errors.New("Quote was not terminated") }
        tok = BToken{BT_WCOMP, S, q.LastColumn.Dtype}
        good = true
    }
    //give interface the right type and append token
    if good {
        //keep wcomp a string if using regex
        if q.Like { q.ANext(); return tok, err }
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

//currently order is only thing after where
func preParseOrder(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_ORDER}
    var err error
    if q.ATok().Id == EOS { return n,nil }
    if q.ATok().Id == KW_ORDER {
        if q.ANext().Id != KW_BY { return n,errors.New("Expected 'by' after 'order'. Found "+q.ATok().Val) }
        q.ANext()
        q.ParseCol = COL_GETIDX
        n.node1,err = preParseColumn(q)
        if err == nil {
            q.NeedAllRows = true
            q.SortCol = q.ParseCol
            q.SortWay = 1
            preParseOrderMethod(q)
        } else { return n,err }
    }
    return n,nil
}

func preParseOrderMethod(q* QuerySpecs) {
    if q.ATok().Id == KW_ORDHOW { q.SortWay = 2 }
}
