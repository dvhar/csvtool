package main

import (
    //"encoding/base64"
    //"crypto/sha1"
  "encoding/csv"
  //"encoding/json"
  . "fmt"
  "os"
  "runtime"
  "errors"
  "sort"
  s "strings"
  "time"
  d "github.com/araddon/dateparse"
  "github.com/pbnjay/memory"
  . "strconv"
  "regexp"

)


var m runtime.MemStats
var totalMem uint64
const (
    T_NULL = 1 << iota
    T_INT = 1 << iota
    T_FLOAT = 1 << iota
    T_DATE = 1 << iota
    T_STRING = 1 << iota
    T_UNKNOWN = 1 << iota
)

/* Using csvQuery function in bigger program. uncomment if using as standalone application
//main func
func main() {
    totalMem = memory.TotalMemory()
    //fname := os.Args[1]

    //get query string from arg
    var fname string
    queryString := os.Args[1]
    if len(os.Args) > 2 { fname = os.Args[2] }

    testQuery := QuerySpecs{
        Qstring : queryString,
        Fname : fname,
    }

    res, err := csvQuery(testQuery);
    if err != nil {Println("csvQuery error:",err)}
    var resCount int
    for i,x := range res.Vals {
        Println(x)
        resCount = i+1
    }
    Printf("Result count: %d\n",resCount)
}
type SingleQueryResult struct {
    Numrows int
    Numcols int
    Types []int
    Colnames []string
    Vals [][]interface{}
    Status int
    Query string
}
*/


func max(a int, b int) int {
    if a>b { return a }
    return b
}

//query specification - file, amount, qtext, qtokens
type QuerySpecs struct {
    Fname string
    Quantity int
    Qstring string
    TokArray []Token
    TokIdx int
    TokWhere int
    SelectColNum int
    SelectAll bool
    ColArray []int
    ColSpec *Columns
    Save bool
    End bool
}
//token retrieval methods
func (q *QuerySpecs) Next() *Token {
    if q.TokIdx < len(q.TokArray)-1 {
        q.TokIdx++
        return &q.TokArray[q.TokIdx]
    } else { q.End = true; return &Token{TOK_END, nil, T_UNKNOWN} }
}
func (q *QuerySpecs) Back() *Token {
    if q.TokIdx > 0 {
        q.TokIdx--
    }
    q.End = false
    return &q.TokArray[q.TokIdx]
}
func (q *QuerySpecs) Reset() {
    q.End = false
    q.TokIdx = 0
}
func (q QuerySpecs) Peek() *Token {
    if q.TokIdx < len(q.TokArray)-1 {
        return &q.TokArray[q.TokIdx+1]
    } else { return &Token{TOK_END, nil, T_UNKNOWN} }
}
func (q QuerySpecs) Tok() *Token {
    if q.End {
        return &Token{TOK_END, nil, T_UNKNOWN}
    } else {
        return &q.TokArray[q.TokIdx]
    }
}
//column metadata
type Columns struct {
    Names []string
    NewNames []string
    Types []int
    NewTypes []int
    Width int
    NewWidth int
}

//get the column count, names, and types of a csv file. arg file pointer or name
func inferTypes(file interface{}) (*Columns, error) {

    //open file or use arg file pointer
    var cols Columns
    var fp *os.File
    var err error
    fp, ok := file.(*os.File)
    if !ok {
        fp,err = os.Open(file.(string))
        if err != nil { return &cols, errors.New("inferTypes: problem opening input file") }
        defer fp.Close()
    }

    cread := csv.NewReader(fp)
    line, err := cread.Read()
    if err != nil { return &cols, errors.New("inferTypes: problem reading input file") }
    //get col names and initialize blank types
    for i,entry := range line {
        cols.Names = append(cols.Names, entry)
        cols.Types = append(cols.Types, 0)
        cols.Width = i+1
    }
    //get samples and infer types from them
    for j:=0;j<10000;j++ {
        line, err := cread.Read()
        if err != nil {break}
        for i,cell := range line {
            entry := s.TrimSpace(cell)
            if entry == "NULL" || entry == "null" || entry == "" {
              cols.Types[i] = max(T_NULL, cols.Types[i])
            } else if _, err := Atoi(entry); err == nil {
              cols.Types[i] = max(T_INT, cols.Types[i])
            } else if _, err := ParseFloat(entry,64); err == nil {
              cols.Types[i] = max(T_FLOAT, cols.Types[i])
            } else if _,err := d.ParseAny(entry); err == nil{
              cols.Types[i] = max(T_DATE, cols.Types[i])
            } else {
              cols.Types[i] = max(T_STRING, cols.Types[i])
            }
        }
    }
    fp.Seek(0,0)
    println("got column data types")
    return &cols, nil
}

func csvQuery(q QuerySpecs) (*SingleQueryResult, error) {
    //tokenize input query
    err := tokenizeQspec(&q)
    if err != nil { Println(err); return &SingleQueryResult{}, err }
    println("tokenized ok")

    //open file
    Println("token array: ",q.TokArray)
    parseFromToken(&q)
    fp,err := os.Open(q.Fname)
    if err != nil { Println(err); return &SingleQueryResult{}, err }
    defer fp.Close()

    //infer column types
    q.ColSpec, err = inferTypes(fp)
    if err != nil { return &SingleQueryResult{}, err }

    //do some pre-query parsing
    err = preParsetokens(&q, 0,1)
    if err != nil { Println(err); return &SingleQueryResult{}, err }
    println("typed the tokens")
    q.Reset()

    //prepare input and output
    totalMem = memory.TotalMemory()
    cread := csv.NewReader(fp)
    result := SingleQueryResult{
        Colnames : q.ColSpec.Names,
        Numcols: q.ColSpec.Width,
        Types: q.ColSpec.Types,
    }
    var tempRow []interface{}
    var row []interface{}
    var limit int
    if q.Quantity == 0 { limit = 1E9 } else { limit = q.Quantity }
    //eat header line, then start looping
    println("running query")
    cread.Read()
    rowsChecked := 0
    for j:=0;j<limit; {
        //read line from csv file and allocate array for it
        line, err := cread.Read()
        if err != nil {break}
        row = make([]interface{}, q.ColSpec.Width)
        //read each cell from line
        for i,cell := range line {
            cell = s.TrimSpace(cell)
            if cell == "NULL" || cell == "null" || cell == "" { row[i] = nil
            } else {
                switch q.ColSpec.Types[i] {
                    case T_INT:    row[i],_ = Atoi(cell)
                    case T_FLOAT:  row[i],_ = ParseFloat(cell,64)
                    case T_DATE:   row[i],_ = d.ParseAny(cell)
                    case T_NULL:   fallthrough
                    case T_STRING: row[i] = cell
                }
            }
        }

        //recursive descent parser finds matches and retrieves results
        match, err := evalQuery(&q, &result, &row, &tempRow)
        if err != nil{ Println("evalQuery error in csvQuery:",err); return &SingleQueryResult{}, err }
        if match { j++; result.Numrows++ }
        q.Reset()

        //watch out for memory ceiling
        runtime.ReadMemStats(&m)
        if m.Alloc > totalMem/3 {
            println("reached soft memory limit")
            messager <- "Not enough memory for all results"
            break
        }

        //periodic updates
        rowsChecked++
        if rowsChecked % 1000 == 0 {
            message := "Scanning line "+Itoa(rowsChecked)+", "+Itoa(j)+" matches so far"
            messager <- message
        }
    }
    //update result column names if only querying a subset of them
    if !q.SelectAll {
        result.Colnames = q.ColSpec.NewNames
        result.Types = q.ColSpec.NewTypes
        result.Numcols = q.ColSpec.NewWidth
    }
    evalOrderBy(&q, &result)
    if q.Save { saver <- chanData{Type : CH_NEXT} }
    messager <- "Finishing a query..."
    return &result, nil
}

func getColumnIdx(colNames []string, column string) (int, error) {
    for i,col := range colNames {
        if s.ToLower(col) == s.ToLower(column) {
            return i, nil
        }
    }
    return 0, errors.New("getColumnIdx: column " + column + " not found")
}

//update index of colum names from full list to selected list
func updateColIdx(oldIdx int, c *Columns) (int, error) {
    if len(c.NewNames) == 0 { return oldIdx, nil }
    name := c.Names[oldIdx]
    var found bool
    var newIdx int
    var err error
    for i,n := range c.NewNames {
        if n == name {
            found = true
            newIdx = i
            break
        }
    }
    if !found { err = errors.New("Could not find index for column") }
    return newIdx, err
}


//query language lexer and parser

const (
    TOK_WCOL = iota
    TOK_SCOL = iota
    TOK_REL = iota
    TOK_VAL = iota
    TOK_LOG = iota
    TOK_OPAREN = iota
    TOK_CPAREN = iota
    TOK_SELECT = iota
    TOK_WHERE = iota
    TOK_TOP = iota
    TOK_ORDER = iota
    TOK_ORDHOW = iota
    TOK_FROM = iota
    TOK_END = iota
    TOK_DISTINCT = iota
)
type Token struct {
    Ttype int
    Val interface{}
    Dtype int
}

//builds a token from a word or words
func tokenMaybeQuoted(word string, token *interface{}, awaitQuote*byte, state int) int {
    if word[0] == '"' || word[0] == '\'' {
        *awaitQuote = word[0]
        if word[len(word)-1] == *awaitQuote {
            //quote is arround just one word
            *token = word[1:len(word)-1]
            *awaitQuote = 0
            return state+1
        //waiting for end quote
        } else {
            *token = word[1:]
            return state
        }
    //still waiting for end quote
    } else if *awaitQuote != 0 && word[len(word)-1] != *awaitQuote {
        *token = (*token).(string) + " " + word
        return state
    //got end quote
    } else if word[len(word)-1] == *awaitQuote {
        *token = (*token).(string) + " " + word[:len(word)-1]
        *awaitQuote = 0
        return state+1
    //no quotes
    } else {
        *token = word
        return state+1
    }
}

//state machine takes QuerySpecs struct with query string and fills out the token array.
//written quickly with no prior knowledge of how to write a language. Should be refactored with state table.
func tokenizeQspec(q *QuerySpecs) error {

    state := 0
    var token interface{}
    var awaitQuote byte
    var needParen int
    var reslice bool
    var between bool
    var noparen string
    var selectPart bool
    var v string
    var toktype int
    var TokArray []Token
    var tempTok Token
    relOperators := map[string]bool {
        "=" : true,
        "!=" : true,
        "<>" : true,
        ">" : true,
        "<" : true,
        "<=" : true,
        ">=" : true,
        // not implemented yet:
        //"like" : true,
        //"unlike" : true,
        //"between" : true,
    }
    logicOperators := map[string]bool {
        "and" : true,
        "or" : true,
    }
    //var needParen int
    comma := regexp.MustCompile(`,`)
    words := s.Fields( comma.ReplaceAllString(q.Qstring, " "))
    if len(words) < 2 { return errors.New("invalid query") }

    for i:= 0; i<len(words); {
        if !reslice {
            v = words[i]
        }
        reslice = false
        if v[0] == '(' { state = 4 }
        if v[0] == ')' { state = 5 }
        if i == 0 && s.ToLower(v) == "select" { selectPart = true; state = 6 }
        if (selectPart || i ==0 ) && s.ToLower(v) == "from" { selectPart = false; state = 10 }
        if (selectPart || i ==0 ) && s.ToLower(v) == "where" { selectPart = false; state = 6 }
        if selectPart && v == "*" { state = 7 }
        if selectPart && v == "DISTINCT" && i < len(words)-1 { state = 11 }
        if v == "order" && i<len(words)-2 && words[i+1] == "by" { state = 8 }

        switch state {

            //column token
            case 0:
                temp := tokenMaybeQuoted(v, &token, &awaitQuote, state)
                if temp != state {
                    if !selectPart {
                        state = 1
                        toktype = TOK_WCOL
                    } else {
                        state = 0
                        toktype = TOK_SCOL
                        q.SelectColNum++
                    }
                    intTok,err := Atoi(token.(string))
                    if err != nil {
                        TokArray = append(TokArray, Token{toktype, token, T_STRING})
                    } else {
                        TokArray = append(TokArray, Token{toktype, intTok, T_INT})
                    }
                }
                i++

            //relop token
            case 1:
                if relOperators[v] {
                    TokArray = append(TokArray, Token{TOK_REL, v, T_UNKNOWN})
                    state = 2
                    if v == "between" { between = true }
                    i++
                } else {
                    return errors.New("tokenizeQspec: bad relational operator '"+v+"' at word ")
                }

            //val token
            case 2:
                //strip parenthesis if touching val token
                noparen = v
                if v[len(v)-1] == ')' && v != ")" {
                    noparen = ""
                    for j,chr := range v {
                        if chr != ')' { noparen = noparen+string(chr)
                        } else { v = v[j:]; reslice = true; state = 5; break }
                    }
                }
                //emit stripped token
                temp := tokenMaybeQuoted(noparen, &token, &awaitQuote, state)
                if temp != state {
                    TokArray = append(TokArray, Token{TOK_VAL, token, T_UNKNOWN})
                    if state != 5 { state = 3 }
                }
                if state != 5 || awaitQuote != 0 { i++ }

            //logop token
            case 3:
                if logicOperators[v] {
                    TokArray = append(TokArray, Token{TOK_LOG, v, T_UNKNOWN})
                    state = 0
                    if between { state = 2; between = false }
                } else {
                    return errors.New("tokenizeQspec: bad logical operator '"+v+"' at word ")
                }
                i++

            //open paren
            case 4:
                needParen++
                TokArray = append(TokArray, Token{TOK_OPAREN, "(", T_UNKNOWN})
                if v[0] == '(' && len(v) > 1 {
                    v = v[1:]
                    reslice = true
                } else {
                    i++
                }
                state = 0

            //close paren
            case 5:
                needParen--
                TokArray = append(TokArray, Token{TOK_CPAREN, ")", T_UNKNOWN})
                if v[0] == ')' && len(v) > 1 {
                    v = v[:len(v)-1]
                    reslice = true
                } else {
                    state = 3
                    i++
                }

            //select and where token
            case 6:
                if selectPart { toktype = TOK_SELECT } else { toktype = TOK_WHERE }
                TokArray = append(TokArray, Token{toktype, s.ToLower(v), T_STRING})
                i++
                if i >= len(words) { return nil }
                if selectPart {
                    switch s.ToLower(words[i]) {
                        case "top": amount, err := Atoi(words[i+1])
                            if err == nil {
                                TokArray = append(TokArray, Token{TOK_TOP, amount, T_INT})
                                i+=2
                            } else { state = 0 }
                        default: state = 0
                    }
                }
                state = 0

            // * token
            case 7:
                TokArray = append(TokArray, Token{TOK_SCOL, -1, T_INT})
                q.SelectColNum++
                state = 0
                i++

            //order by token
            case 8:
                intTok,err := Atoi(words[i+2])
                if err != nil {
                    TokArray = append(TokArray, Token{TOK_ORDER, words[i+2], T_STRING})
                } else {
                    TokArray = append(TokArray, Token{TOK_ORDER, intTok, T_INT})
                }
                state = 9
                i += 3

            //order ASC or DESC token
            case 9:
                tempTok = Token{TOK_ORDHOW, 0, T_INT}
                switch s.ToLower(v) {
                    case "desc": tempTok.Val = 0
                    case "asc": tempTok.Val = 1
                    default: return errors.New("unexpected word after order by")
                }
                TokArray = append(TokArray, tempTok)
                state = 0
                i++

            //from token
            case 10:
                TokArray = append(TokArray, Token{TOK_FROM, words[i+1], T_STRING})
                selectPart = false
                state = 6
                i += 2

            //disctinct token
            case 11:
                TokArray = append(TokArray, Token{TOK_DISTINCT, 0, T_INT})
                i++
        }
    }
    q.TokArray = TokArray
    return nil
}

func parseFromToken(q *QuerySpecs) error {
    tok := q.Tok()
    if tok.Ttype == TOK_END {
        if q.Fname == "" { q.Reset(); return errors.New("No file to query")
        } else { q.Reset(); return nil }
    } else if tok.Ttype == TOK_FROM {
        q.Fname = tok.Val.(string)
        q.Reset();
        println("found from val: "+q.Fname)
        return nil
    } else { q.Next(); return parseFromToken(q) }
}

//give val tokens their correct Dtype and change col name tokens to col index
func preParsetokens(q *QuerySpecs, col int, counter int) error {
    tok := q.Tok()
    var ok bool
    var err error

    //record "where" position
    if tok.Ttype == TOK_WHERE { q.TokWhere = counter; println("found where token at postition "+Itoa(counter)) }

    //parse column tokens, build column list
    if tok.Ttype == TOK_WCOL || tok.Ttype == TOK_SCOL || tok.Ttype == TOK_ORDER {
        col, ok = tok.Val.(int)
        //change column token value to index of column
        if !ok {
            col, err = getColumnIdx(q.ColSpec.Names, tok.Val.(string))
            if err != nil { return errors.New("preParsetokens: column "+tok.Val.(string)+" not found") }
            println("column "+tok.Val.(string)+" is number "+Itoa(col))
        } else {
            col--
        }
        tok.Val = col
        //add select column to list, update result column names and types
        if col >= q.ColSpec.Width { return errors.New("column index too big:"+Itoa(col)+" > "+Itoa(q.ColSpec.Width)) }
        if tok.Ttype == TOK_SCOL && col >= 0 {
            q.ColArray = append(q.ColArray, col)
            q.ColSpec.NewNames = append(q.ColSpec.NewNames, q.ColSpec.Names[col])
            q.ColSpec.NewTypes = append(q.ColSpec.NewTypes, q.ColSpec.Types[col])
        }
        if tok.Ttype == TOK_SCOL && col < 0 { q.SelectAll = true }
    }

    //set row limit if TOP token found
    if tok.Ttype == TOK_TOP { q.Quantity = tok.Val.(int) }

    //give val token the type of its column, or null if it is null
    if tok.Ttype == TOK_VAL {
        tok.Dtype = q.ColSpec.Types[col]
        if s.ToLower(tok.Val.(string)) == "null" { tok.Dtype = T_NULL }
        switch tok.Dtype {
            case T_INT:    tok.Val,err = Atoi(tok.Val.(string))
            case T_FLOAT:  tok.Val,err = ParseFloat(tok.Val.(string), 64)
            case T_DATE:   tok.Val,err = d.ParseAny(tok.Val.(string))
            case T_NULL:   tok.Val = nil
            case T_STRING: tok.Val = tok.Val.(string)
        }
        if err != nil { return err }
    }
    tok = q.Next()
    if tok.Ttype != TOK_END {
        err = preParsetokens(q, col, counter+1)
    } else {
        //end of token list - send header to realtime saver
        if len(q.ColSpec.NewNames) == 0 {
            q.SelectAll = true
            saver <- chanData{Type : CH_HEADER, Header : q.ColSpec.Names}
        } else {
            saver <- chanData{Type : CH_HEADER, Header : q.ColSpec.NewNames}
        }
    }
    return err
}

//top level recursive descent parser for select statement
func evalQuery(q *QuerySpecs, result *SingleQueryResult, row *[]interface{}, selected *[]interface{}) (bool,error) {

    //see if row matches expression
    match, err := evalWhere(q, row)
    if err != nil{ Println("evalMultiComparison error in evalQuery:",err); return false, err }
    if !match { return false, nil }

    //copy entire row if there is no select section
    if q.SelectAll {
        result.Vals = append(result.Vals, *row)
        if q.Save { saver <- chanData{Type : CH_ROW, Row : row} }
        return true, nil
    }

    //find select colum token, return error if none found
    q.Reset()
    for ;q.Peek().Ttype != TOK_SCOL || q.Peek().Ttype == TOK_END; q.Next() {}
    if q.Peek().Ttype == TOK_END { return false, errors.New("No select column found") }

    //retreive the selected columns
    countSelected := evalSelectCol(q, result, row, selected, 0)
    if countSelected != q.SelectColNum { return true, errors.New("returned the wrong number of columns") }
    return true, nil
}

//recursive descent parser for selected columns
func evalSelectCol(q *QuerySpecs, result *SingleQueryResult, row *[]interface{}, selected *[]interface{}, count int) int {
    tok := q.Next()
    if tok.Ttype != TOK_SCOL { return count }

    //add col to selected array if regular column found
    if count < q.SelectColNum {
        if count == 0 { *selected = make([]interface{}, q.SelectColNum) }
        (*selected)[count] = (*row)[tok.Val.(int)]
        if count == q.SelectColNum - 1 {
            result.Vals = append(result.Vals, *selected)
            if q.Save { saver <- chanData{Type : CH_ROW, Row : selected} }
            q.ColSpec.NewWidth = count+1
        }
    }
    return evalSelectCol(q, result, row, selected, count+1)
}

//evaluate to true if there is no where clause, else call evalMultiComparison
func evalWhere(q *QuerySpecs, entry *[]interface{}) (bool, error) {
    if q.Tok().Ttype == TOK_FROM { q.Next() }
    if q.Tok().Ttype == TOK_SELECT && q.TokWhere == 0 {
        return true, nil
    }
    return evalMultiComparison(q, entry)
}

//recursive descent parser for where comparison part of query
func evalMultiComparison(q *QuerySpecs, entry *[]interface{}) (bool, error) {

    //see if you're starting at select, where, or word after where
    match := false
    var err error
    tok := q.Tok()
    if tok.Ttype == TOK_SELECT {
        q.TokIdx = q.TokWhere
        tok = q.Tok()
    } else if tok.Ttype == TOK_WHERE {
        tok = q.Next()
    }

    //if first token is column, evaluate it
    if tok.Ttype == TOK_WCOL {
        match, err = evalComparison(q, entry)
        if err != nil { return false, err }
        if q.Peek().Ttype == TOK_END || q.Peek().Ttype == TOK_CPAREN || q.Peek().Ttype == TOK_ORDER {
            return match, err
        }

    //end of tok == col. now elif tok == (
    } else if tok.Ttype == TOK_OPAREN {
        q.Next()
        match, err = evalMultiComparison(q,entry)
        //eat closing paren, return if this expression is done
        q.Next()
        if q.Peek().Ttype == TOK_END || q.Peek().Ttype == TOK_CPAREN {
            return match, err
        }
    }

    //if logical operator, perform logical operation with next comparision result
    if q.Peek().Ttype == TOK_LOG {
        logop := q.Next().Val.(string)
        q.Next()
        nextExpr, err := evalMultiComparison(q,entry)
        if err != nil { return false, err }
        switch logop {
            case "and": match = match && nextExpr
            case "or":  match = match || nextExpr
        }
    }
    return match, err
}

//perform comparision of column entry with given value
func evalComparison(q *QuerySpecs, entry *[]interface{}) (bool,error) {
    match := false
    negate := false
    compCol := q.Tok()
    relop := q.Next()
    compVal := q.Next()
    //if comparing non-null values
    if compVal.Val != nil && (*entry)[compCol.Val.(int)] != nil {
        switch relop.Val.(string) {
            case "<>": fallthrough
            case "!=": negate = true
                       fallthrough
            case "=" :
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = compVal.Val.(string) == (*entry)[compCol.Val.(int)].(string)
                    case T_INT:    match = compVal.Val.(int) == (*entry)[compCol.Val.(int)].(int)
                    case T_FLOAT:  match = compVal.Val.(float64) == (*entry)[compCol.Val.(int)].(float64)
                    case T_DATE:   match = compVal.Val.(time.Time).Equal((*entry)[compCol.Val.(int)].(time.Time))
                }
            case "<=": negate = true
                       fallthrough
            case ">" :
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = (*entry)[compCol.Val.(int)].(string) > compVal.Val.(string)
                    case T_INT:    match = (*entry)[compCol.Val.(int)].(int) > compVal.Val.(int)
                    case T_FLOAT:  match = (*entry)[compCol.Val.(int)].(float64) > compVal.Val.(float64)
                    case T_DATE:   match = (*entry)[compCol.Val.(int)].(time.Time).After(compVal.Val.(time.Time))
                }
            case ">=" : negate = true
                       fallthrough
            case "<":
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = (*entry)[compCol.Val.(int)].(string) < compVal.Val.(string)
                    case T_INT:    match = (*entry)[compCol.Val.(int)].(int) < compVal.Val.(int)
                    case T_FLOAT:  match = (*entry)[compCol.Val.(int)].(float64) < compVal.Val.(float64)
                    case T_DATE:   match = (*entry)[compCol.Val.(int)].(time.Time).Before(compVal.Val.(time.Time))
                }
        }

    //if comparing to null
    } else if compVal.Val == nil {
        switch relop.Val.(string) {
            case "<>": fallthrough
            case "!=": negate = true
                       fallthrough
            case "=" : match = (*entry)[compCol.Val.(int)] == nil
            default  : return false, errors.New("Invalid operation with null: "+relop.Val.(string)+". Valid operators: = != <>")
        }
    }
    if negate { match = !match }
    return match, nil
}

//evaluate order-by clause by calling sortBy functin
func evalOrderBy(q *QuerySpecs, result *SingleQueryResult) error {
    for  { if q.Peek().Ttype == TOK_ORDER || q.Peek().Ttype == TOK_END { break } else { q.Next() } }
    tok := q.Next()
    whichWay := 0
    if q.Peek().Ttype == TOK_ORDHOW { whichWay = q.Peek().Val.(int) }
    var err error
    if tok.Ttype == TOK_ORDER {
        tok.Val, err = updateColIdx(tok.Val.(int), q.ColSpec)
        return sortBy(result, tok.Val.(int), whichWay)
    }
    return err
}

//sort query results by column name or index
func sortBy(res *SingleQueryResult, colName interface{}, whichWay int) error {
    colIndex, ok := colName.(int)
    var err error
    if !ok {
        colIndex, err = getColumnIdx(res.Colnames, colName.(string))
        if err != nil { return err }
    }
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
            if whichWay == 1 { return !ret }
            return ret
        }
        return false
    })
    return nil
}
