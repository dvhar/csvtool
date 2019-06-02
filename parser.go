//file provides the parseQuery function
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
    colSpec Columns
    fname string
    queryString string
    tokArray []Token
    tokIdx int
    quantityLimit int
    quantityRetrieved int
    distinctIdx int
    selectAll bool
    sortCol int
    sortWay int
    save bool
    like bool
    parseCol int
    showLimit int
    lastColumn treeTok
    tree *Node
    tempVal interface{}
}
func (q *QuerySpecs) NextTok() *Token {
    if q.tokIdx < len(q.tokArray)-1 { q.tokIdx++ }
    return &q.tokArray[q.tokIdx]
}
func (q QuerySpecs) PeekTok() *Token {
    if q.tokIdx < len(q.tokArray)-1 {
        return &q.tokArray[q.tokIdx+1]
    } else {
        println("end of tokens")
        return &q.tokArray[q.tokIdx]
    }
}
func (q QuerySpecs) Tok() *Token { return &q.tokArray[q.tokIdx] }
func (q *QuerySpecs) Reset() { q.tokIdx = 0 }
const (
    //parse tree node types
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
    node1 *Node
    node2 *Node
    node3 *Node
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
    COL_ADD = iota
    COL_GETIDX = iota
)
type treeTok struct {
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
    q.selectAll = true
    q.colSpec.NewNames = q.colSpec.Names
    q.colSpec.NewTypes = q.colSpec.Types
    q.colSpec.NewWidth = q.colSpec.Width
    q.colSpec.NewPos = make([]int,q.colSpec.Width)
    for i,_ := range q.colSpec.NewNames { q.colSpec.NewPos[i] = i+1 }
}
func newCol(q* QuerySpecs,ii int) {
    if !q.selectAll {
        q.colSpec.NewNames = append(q.colSpec.NewNames, q.colSpec.Names[ii])
        q.colSpec.NewTypes = append(q.colSpec.NewTypes, q.colSpec.Types[ii])
        q.colSpec.NewPos = append(q.colSpec.NewPos, ii+1)
        q.colSpec.NewWidth++
    }
}

//get column types from first 10000 rows
func inferTypes(q *QuerySpecs) error {

    //open file
    fp,err := os.Open(q.fname)
    if err != nil { return errors.New("problem opening input file") }
    defer func(){ fp.Seek(0,0); fp.Close() }()

    cread := csv.NewReader(fp)
    line, err := cread.Read()
    if err != nil { return errors.New("problem reading input file") }
    //get col names and initialize blank types
    for i,entry := range line {
        q.colSpec.Names = append(q.colSpec.Names, entry)
        q.colSpec.Types = append(q.colSpec.Types, 0)
        q.colSpec.Width = i+1
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
              q.colSpec.Types[i] = max(T_NULL, q.colSpec.Types[i])
            } else if LeadingZeroString.MatchString(entry) {
              q.colSpec.Types[i] = T_STRING
            } else if _, err := Atoi(entry); err == nil {
              q.colSpec.Types[i] = max(T_INT, q.colSpec.Types[i])
            } else if _, err := ParseFloat(entry,64); err == nil {
              q.colSpec.Types[i] = max(T_FLOAT, q.colSpec.Types[i])
            } else if _,err := d.ParseAny(entry); err == nil{
              q.colSpec.Types[i] = max(T_DATE, q.colSpec.Types[i])
            } else {
              q.colSpec.Types[i] = T_STRING
            }
        }
    }
    println("got column data types")
    return  err
}

//open file and call type inferrer
func evalFrom(q *QuerySpecs) error {
    //go straight to the from token or end
    if q.Tok().Id != KW_SELECT { return errors.New("Query must start with select. found "+q.Tok().Val) }
    for ; q.Tok().Id != KW_FROM && q.Tok().Id != EOS ; {q.NextTok()}
    if q.Tok().Id == EOS && q.fname == "" { return errors.New("Could not find a valid 'from file' part of query") }
    if q.Tok().Id == EOS && q.fname != "" { return inferTypes(q) }
    if q.Tok().Id == KW_FROM && q.PeekTok().Id != WORD {
        return errors.New("Unexpected token after 'from': "+q.PeekTok().Val) }
    if q.Tok().Id == KW_FROM && q.PeekTok().Id == WORD {
        q.fname = q.PeekTok().Val
        err := inferTypes(q)
        q.Reset()
        return err
    }
    return errors.New("Unknown problem parsing 'from file' part of query")
}

//recursive descent parser builds parse tree and QuerySpecs
func parseQuery(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_PPTOKENS}
    n.tok1 = q
    //first turn query string into A tokens
    lineNo = 1
    err := scanTokens(q)
    if err != nil { return n,err }

    //then open file and get column info
    err = evalFrom(q)
    if err != nil { return n,err }

    //select section
    n.node1,err =  parseSelect(q)
    if err != nil { return n,err }

    //skip from section for now because already evaluated
    parseFrom(q)

    //where section
    n.node3,err =  parseWhere(q)
    if err != nil { return n,err }

    //Order by
    err =  parseOrder(q)
    return n,err
}

//node1 is selections
func parseSelect(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_SELECT}
    var err error
    if q.Tok().Id != KW_SELECT { return n,errors.New("Expected 'select' token. found "+q.Tok().Val) }
    q.NextTok()
    err = parseTop(q)
    if err != nil { return n,err }
    countSelected = 0
    n.node1,err = parseSelections(q)
    return n,err
}

func parseTop(q* QuerySpecs) error {
    //terminal
    var err error
    if q.Tok().Id == KW_TOP {
        q.quantityLimit, err = Atoi(q.PeekTok().Val)
        if err != nil { return errors.New("Expected number after 'top'. found "+q.PeekTok().Val) }
        q.NextTok(); q.NextTok()
    }
    return nil
}

//tok1 is selected column
//tok2 is destination column index
//node1 is next selection
var countSelected int
func parseSelections(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_SELECTIONS}
    var err error
    switch q.Tok().Id {
        case SP_ALL:
            selectAll(q)
            q.NextTok()
            return parseSelections(q)
        //non-column words in select section
        case KW_DISTINCT:
            return parseSpecial(q)
        //column
        case WORD: fallthrough
        case SP_SQUOTE: fallthrough
        case SP_DQUOTE:
            q.parseCol = COL_ADD
            n.tok1,err = parseColumn(q)
            if err != nil { return n,err }
            n.tok2 = countSelected
            countSelected++
            n.node1,err = parseSelections(q)
            return n,err
        case KW_FROM:
            if q.colSpec.NewWidth == 0 { selectAll(q) }
    }
    return n,nil
}

//returns column tok
func parseColumn(q* QuerySpecs) (treeTok,error) {
    var ii int
    var err error
    switch q.Tok().Id {
        //parse selected column
        case WORD:
            c := q.Tok().Val
            ii, err = Atoi(c)
            //if it's a number
            if err == nil {
                if ii > q.colSpec.Width { return treeTok{},errors.New("Column number too big: "+c+". Max is "+Itoa(q.colSpec.Width)) }
                if ii < 1 { return treeTok{},errors.New("Column number too small: "+c) }
                ii -= 1
            //if it's a name
            } else {
                ii, err = getColumnIdx(q.colSpec.Names, c)
            }
        //parse column from quotes
        case SP_SQUOTE: fallthrough
        case SP_DQUOTE:
            quote := q.Tok().Id
            var S string
            for ; q.NextTok().Id != quote && q.Tok().Id != EOS; { S += q.Tok().Val }
            if q.Tok().Id == EOS { return treeTok{},errors.New("Quote was not terminated") }
            ii, err = getColumnIdx(q.colSpec.Names, S)
    }
    if err != nil { return treeTok{},err }
    //see if adding column to colSpec
    if q.parseCol == COL_ADD { newCol(q, ii) }
    q.parseCol = ii
    q.NextTok()
    return treeTok{0, ii, q.colSpec.Types[ii]},err
}

//tok1 is selected column if distinct
//tok2 is type of special
func parseSpecial(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_SPECIAL}
    var err error
    switch q.Tok().Id {
        case KW_DISTINCT:
            q.NextTok()
            q.parseCol = COL_GETIDX
            n.tok1,err = parseColumn(q)
            if err != nil { return n,err }
            n.tok2 = countSelected
            countSelected++
            q.distinctIdx = q.parseCol
            if !q.selectAll { newCol(q, q.parseCol) }
            n.node1,err = parseSelections(q)
            n.label = N_SELECTIONS
            return n,err
    }
    return n,errors.New("Unexpected token in 'select' section:"+q.Tok().Val)
}

func parseFrom(q* QuerySpecs) error {
    if q.Tok().Id != KW_FROM { return errors.New("Expected 'from'. Found: "+q.Tok().Val) }
    q.NextTok()
    q.NextTok()
    if q.Tok().Id == KW_AS {
        q.NextTok()
        q.NextTok()
    }
    return nil
}

//node1 is conditions
func parseWhere(q*QuerySpecs) (*Node,error) {
    n := &Node{label:N_WHERE}
    var err error
    if q.Tok().Id != KW_WHERE { return n,nil }
    q.NextTok()
    n.node1,err = parseConditions(q)
    return n,err
}

//tok1 is negate
//node1 is condition or conditions
//node2 is more conditions
func parseConditions(q*QuerySpecs) (*Node,error) {
    n := &Node{label:N_CONDITIONS}
    var err error
    if q.Tok().Id == SP_NEGATE {
        q.NextTok()
        n.tok1 = SP_NEGATE
    }
    switch q.Tok().Id {
        case SP_LPAREN:
            tok := q.Tok()
            q.NextTok();
            n.node1,err = parseConditions(q)
            if err != nil { return n,err }
            tok = q.Tok()
            if tok.Id != SP_RPAREN { return n,errors.New("No closing parentheses. Found: "+tok.Val) }
            q.NextTok()
            n.node2,err = preparseMore(q)
            return n,err
        case WORD: fallthrough
        case SP_DQUOTE: fallthrough
        case SP_SQUOTE:
            //get column index before next step
            q.parseCol = COL_GETIDX
            _,err = parseColumn(q)
            if err != nil { return n,err }
            q.lastColumn = treeTok{0, q.parseCol, q.colSpec.Types[q.parseCol]}
            //see if comparison is normal or between
            if q.Tok().Id == KW_BETWEEN || q.PeekTok().Id == KW_BETWEEN {
                n.node1, err = parseBetween(q)
            } else {
                n.node1, err = parseCompare(q)
            }
            if err != nil { return n,err }
            n.node2,err = preparseMore(q)
            return n,err
    }
    return n,errors.New("Unexpected token in 'where' section: "+q.Tok().Val)
}

//tok1 is column to compare
//node1 is relop with comparision
func parseCompare(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_COMPARE}
    var err error
    n.tok1 = q.lastColumn
    n.node1,err = parseRel(q)
    return n,err
}

//tok1 is negate
//tok2 is relop
//tok3 is comparison value
func parseRel(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_REL}
    //negater before relop
    if q.Tok().Id == SP_NEGATE {
        q.NextTok()
        n.tok1 = SP_NEGATE
    }
    //relop and value
    if (q.Tok().Id & RELOP) != 0 {
        tok := q.Tok()
        if tok.Id == KW_LIKE { q.like = true; }
        n.tok2 = treeTok{tok.Id, tok.Val, 0}
        q.NextTok()
        btok,err := tokFromQuotes(q)
        //if relop is 'like', compile a regex
        if q.like {
            q.like = false
            re := regexp.MustCompile("%")
            btok.Val = re.ReplaceAllString(Sprint(btok.Val), ".*")
            re = regexp.MustCompile("_")
            btok.Val = re.ReplaceAllString(Sprint(btok.Val), ".")
            btok.Val,err = regexp.Compile("(?i)^"+btok.Val.(string)+"$")
        }
        n.tok3 = btok
        return n,err
    }
    return n,errors.New("Expected relational operator. Found: "+q.Tok().Val)
}

//tok1 is logical operator
//node1 is next condition
func preparseMore(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_MORE}
    var err error
    if (q.Tok().Id & LOGOP) == 0 { return n,nil }
    n.tok1 = q.Tok().Id
    q.NextTok()
    n.node1,err = parseConditions(q)
    return n,err
}

//return parse tree segment for between as conditions node
func parseBetween(q* QuerySpecs) (*Node,error) {
    n := &Node{label:N_CONDITIONS}
    var  val1, val2, relop1, relop2 treeTok
    var firstSmaller bool
    var err error
    //negation and get to value token
    if q.Tok().Id == SP_NEGATE { n.tok1 = SP_NEGATE; q.NextTok() }
    q.NextTok()
    val1, err = tokFromQuotes(q)
    if err != nil { return n,err }
    if q.Tok().Id != KW_AND { return n,errors.New("Expected 'and' in between clause, got "+q.Tok().Val) }
    q.NextTok()
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
        relop1 = treeTok{SP_GREATEQ, ">=", 0}
        relop2 = treeTok{SP_LESS, "<", 0}
    } else {
        relop1 = treeTok{SP_LESS, "<", 0}
        relop2 = treeTok{SP_GREATEQ, ">=", 0}
    }

    //parse tree for 2 comparisions
    n.node1 = &Node{label: N_COMPARE, tok1: q.lastColumn,
        node1: &Node{label: N_REL, tok2: relop1, tok3: val1},
    }
    n.node2 = &Node{label: N_MORE, tok1: KW_AND,
        node1: &Node{label: N_CONDITIONS,
            node1: &Node{label: N_COMPARE, tok1: q.lastColumn,
                node1: &Node{label: N_REL, tok2: relop2, tok3: val2},
            },
            node2: &Node{label: N_MORE},
        },
    }
    return n,err
}

//comparison values in where section
func tokFromQuotes(q* QuerySpecs) (treeTok,error) {
    var good bool
    var tok treeTok
    var err error
    //add to array if just a word
    if q.Tok().Id == WORD {
        tok = treeTok{0, q.Tok().Val, q.lastColumn.Dtype}
        good = true
    }
    //construct string from values between quotes
    if q.Tok().Id == SP_SQUOTE || q.Tok().Id == SP_DQUOTE {
        quote := q.Tok().Id
        var S string
        for ; q.NextTok().Id != quote && q.Tok().Id != EOS; { S += q.Tok().Val }
        if q.Tok().Id == EOS { return tok, errors.New("Quote was not terminated") }
        tok = treeTok{0, S, q.lastColumn.Dtype}
        good = true
    }
    //give interface the right type
    if good {
        //keep wcomp a string if using regex
        if q.like { q.NextTok(); return tok, err }
        if s.ToLower(tok.Val.(string)) == "null" { tok.Dtype = T_NULL }
        switch tok.Dtype {
            case T_INT:    tok.Val,err = Atoi(tok.Val.(string))
            case T_FLOAT:  tok.Val,err = ParseFloat(tok.Val.(string), 64)
            case T_DATE:   tok.Val,err = d.ParseAny(tok.Val.(string))
            case T_NULL:   tok.Val = nil
            case T_STRING: tok.Val = tok.Val.(string)
        }
        q.NextTok()
        return tok, err
    }
    return tok, errors.New("Expected a comparision value but got "+q.Tok().Val)
}

//currently order is only thing after where
func parseOrder(q* QuerySpecs) error {
    var err error
    if q.Tok().Id == EOS { return nil }
    if q.Tok().Id == KW_ORDER {
        if q.NextTok().Id != KW_BY { return errors.New("Expected 'by' after 'order'. Found "+q.Tok().Val) }
        q.NextTok()
        q.parseCol = COL_GETIDX
        _,err = parseColumn(q)
        if err == nil {
            q.sortCol = q.parseCol
            q.sortWay = 1
            if q.Tok().Id == KW_ORDHOW { q.sortWay = 2 }
        } else { return err }
    }
    return nil
}
