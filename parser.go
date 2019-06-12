//file provides the parseQuery function
// _fmk0# is file map key designed to avoid collisions with aliases and file names
package main
import (
  "regexp"
  "encoding/csv"
  "path/filepath"
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
	lastColumn TreeTok
	tree *Node
	files map[string]*FileData
	numfiles int
	tempVal interface{}
	fromRow []interface{}
	toRow []interface{}
	intColumn bool
}
func (q *QuerySpecs) NextTok() *Token {
	if q.tokIdx < len(q.tokArray)-1 { q.tokIdx++ }
	return &q.tokArray[q.tokIdx]
}
func (q QuerySpecs) PeekTok() *Token {
	if q.tokIdx < len(q.tokArray)-1 { return &q.tokArray[q.tokIdx+1] }
	return &q.tokArray[q.tokIdx]
}
func (q QuerySpecs) Tok() *Token { return &q.tokArray[q.tokIdx] }
func (q *QuerySpecs) Reset() { q.tokIdx = 0 }
const (
	//parse tree node types
	N_QUERY = iota
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
	N_COLITEM = iota
    N_EXPRADD = iota
    N_EXPRMULT = iota
    N_EXPRNEG = iota
    N_EXPRCASE = iota
	N_CPREDLIST = iota
	N_CPRED = iota
	N_CWEXPRLIST = iota
	N_CWEXPR = iota
	N_PREDICATES = iota
	N_PREDCOMP = iota
	N_VALUE = iota
)
type FileData struct {
	fname string
	names []string
	types []int
	width int
}
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
	NewNames []string
	NewTypes []int
	NewPos []int
	NewWidth int
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
type TreeTok struct {
	id int
	val interface{}
	dtype int
}
func max(a int, b int) int {
	if a>b { return a }
	return b
}
func getColumnIdx(colNames []string, column string) (int, error) {
	for i,col := range colNames {
		if s.ToLower(col) == s.ToLower(column) { return i, nil }
	}
	return 0, errors.New("Column " + column + " not found")
}
func selectAll(q* QuerySpecs) {
	q.selectAll = true
	q.colSpec.NewNames = make([]string,0)
	q.colSpec.NewTypes = make([]int,0)
	for i := 1; i <= q.numfiles; i++ {
		q.colSpec.NewNames = append(q.colSpec.NewNames, q.files["_fmk0"+Itoa(i)].names...)
		q.colSpec.NewTypes = append(q.colSpec.NewTypes, q.files["_fmk0"+Itoa(i)].types...)
		q.colSpec.NewWidth += q.files["_fmk0"+Itoa(i)].width
	}
	q.colSpec.NewPos = make([]int,q.colSpec.NewWidth)
	for i,_ := range q.colSpec.NewNames { q.colSpec.NewPos[i] = i+1 }
}
func newCol(q* QuerySpecs,ii int) {
	if !q.selectAll {
		q.colSpec.NewNames = append(q.colSpec.NewNames, q.files["_fmk01"].names[ii])
		q.colSpec.NewTypes = append(q.colSpec.NewTypes, q.files["_fmk01"].types[ii])
		q.colSpec.NewPos = append(q.colSpec.NewPos, ii+1)
		q.colSpec.NewWidth++
	}
}

//infer type of single string value
var LeadingZeroString *regexp.Regexp
func getNarrowestType(value string, startType int) int {
	entry := s.TrimSpace(value)
	if s.ToLower(entry) == "null" || entry == "NA" || entry == "" {
	  startType = max(T_NULL, startType)
	} else if LeadingZeroString.MatchString(entry) {
	  startType = T_STRING
	} else if _, err := Atoi(entry); err == nil {
	  startType = max(T_INT, startType)
	} else if _, err := ParseFloat(entry,64); err == nil {
	  startType = max(T_FLOAT, startType)
	} else if _,err := d.ParseAny(entry); err == nil{
	  startType = max(T_DATE, startType)
	} else {
	  startType = T_STRING
	}
	return startType
}
//infer types of all infile columns
func inferTypes(q *QuerySpecs, f string) error {
	LeadingZeroString = regexp.MustCompile(`^0\d+$`)
	//open file
	fp,err := os.Open(q.files[f].fname)
	if err != nil { return errors.New("problem opening input file") }
	defer func(){ fp.Seek(0,0); fp.Close() }()
	cread := csv.NewReader(fp)
	line, err := cread.Read()
	if err != nil { return errors.New("problem reading input file") }
	//get col names and initialize blank types
	for i,entry := range line {
		q.files[f].names = append(q.files[f].names, entry)
		q.files[f].types = append(q.files[f].types, 0)
		q.files[f].width = i+1
	}
	//get samples and infer types from them
	for j:=0;j<10000;j++ {
		line, err := cread.Read()
		if err != nil { break }
		for i,cell := range line {
			q.files[f].types[i] = getNarrowestType(cell, q.files[f].types[i])
		}
	}
	return  err
}
//find files and open them
func openFiles(q *QuerySpecs) error {
	extension := regexp.MustCompile(`\.csv$`)
	q.files = make(map[string]*FileData)
	q.numfiles = 0
	for ; q.Tok().id != EOS ; q.NextTok() {
		_,err := os.Stat(q.Tok().val)
		//open file and add to file map
		if err == nil && extension.MatchString(q.Tok().val) {
			file := &FileData{fname : q.Tok().val}
			filename := filepath.Base(file.fname)
			q.numfiles++
			key := "_fmk0" + Sprint(q.numfiles)
			q.files[key] = file
			q.files[filename[:len(filename)-4]] = file
			if q.NextTok().id == WORD { q.files[q.Tok().val] = file }
			if q.Tok().id == KW_AS && q.NextTok().id == WORD { q.files[q.Tok().val] = file }
			if inferTypes(q, key) != nil {return err}
		}
	}
	q.Reset()
	return nil
}

//recursive descent parser builds parse tree and QuerySpecs
func parseQuery(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_QUERY}
	n.tok1 = q
	lineNo = 1
	err := scanTokens(q)
	if err != nil { return n,err }
	err = openFiles(q)
	if err != nil { return n,err }

	//new expression parser test
	n.node1,err =  parse2Select(q)
	if err != nil { return n,err }
	_,_,_,err = typeCheck(n.node1)
	if err != nil {Println("err:",err); return n,err }
	branchShortener(q, n.node1)
	columnNamer(q, n.node1)
	treePrint(n.node1,0)
	//Println("newparser finished with",q.Tok())
	//q.Reset()

	//n.node1,err =  parseSelect(q)
	//if err != nil { return n,err }
	n.node2, err = parseFrom(q)
	if err != nil { return n,err }
	n.node3,err =  parseWhere(q)
	if err != nil { return n,err }
	err =  parseOrder(q)
	if err != nil { return n,err }
	if q.Tok().id != EOS { err = errors.New("Expected end of query, got "+q.Tok().val) }
	return n,err
}

//node1 is selections
func parseSelect(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECT}
	var err error
	if q.Tok().id != KW_SELECT { return n,errors.New("Expected query to start with 'select'. Found "+q.Tok().val) }
	q.NextTok()
	err = parseTop(q)
	if err != nil { return n,err }
	countSelected = 0
	n.node1,err = parseSelections(q)
	return n,err
}

//row limit
func parseTop(q* QuerySpecs) error {
	var err error
	if q.Tok().id == KW_TOP {
		q.quantityLimit, err = Atoi(q.PeekTok().val)
		if err != nil { return errors.New("Expected number after 'top'. Found "+q.PeekTok().val) }
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
	switch q.Tok().id {
	case SP_STAR:
		selectAll(q)
		q.NextTok()
		return parseSelections(q)
	//non-column words in select section
	case KW_DISTINCT:
		return parseSpecial(q)
	//column
	case WORD:
		q.parseCol = COL_ADD
		n.tok1,err = columnParser(q)
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

//parse column and file key from quotes and/or dot notation
func dotParser(q* QuerySpecs, dot bool) (string,string,error) {
	var S string
	key := "_fmk01"
	S = q.Tok().val
	split := s.SplitAfterN(S, ".", 2)
	//see if doing dot notation
	if dot && len(split) > 1 {
		key = s.TrimRight(split[0], ".")
		S = split[1]
		_,ok := q.files[key]
		if !ok { return "","", errors.New(key+" is not a file alias") }
	}
	q.NextTok()
	return key, S, nil
}

//returns column tok
func columnParser(q* QuerySpecs) (TreeTok,error) {
	var ii int
	key, col, err := dotParser(q, true)
	if err != nil { return TreeTok{},err }
	//if it's a number
	ii, err = Atoi(col)
	if err == nil {
		if ii > q.files[key].width { return TreeTok{},errors.New("Column number too big: "+col+". Max is "+Itoa(q.files[key].width)) }
		if ii < 1 { return TreeTok{},errors.New("Column number too small: "+col) }
		ii -= 1
	//else it's a name
	} else { ii, err = getColumnIdx(q.files[key].names, col) }
	if err != nil { return TreeTok{},err }
	if q.parseCol == COL_ADD { newCol(q, ii) }
	q.parseCol = ii
	return TreeTok{0, ii, q.files[key].types[ii]},err
}

//tok1 is selected column if distinct
//tok2 is type of special
func parseSpecial(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SPECIAL}
	var err error
	switch q.Tok().id {
	case KW_DISTINCT:
		q.NextTok()
		q.parseCol = COL_GETIDX
		n.tok1,err = columnParser(q)
		if err != nil { return n,err }
		n.tok2 = countSelected
		countSelected++
		q.distinctIdx = q.parseCol
		if !q.selectAll { newCol(q, q.parseCol) }
		n.node1,err = parseSelections(q)
		n.label = N_SELECTIONS
		return n,err
	}
	return n,errors.New("Unexpected token in 'select' section:"+q.Tok().val)
}

//tok1 is file path
//tok2 is alias
func parseFrom(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_FROM}
	if q.Tok().id != KW_FROM { return n,errors.New("Expected 'from'. Found: "+q.Tok().val) }
	n.tok1 = q.NextTok()
	q.NextTok()
	if q.Tok().id == WORD {
		n.tok2 = q.Tok()
		q.NextTok()
	}
	if q.Tok().id == KW_AS {
		n.tok2 = q.NextTok()
		q.NextTok()
	}
	return n, nil
}

//node1 is conditions
func parseWhere(q*QuerySpecs) (*Node,error) {
	n := &Node{label:N_WHERE}
	var err error
	if q.Tok().id != KW_WHERE { return n,nil }
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
	if q.Tok().id == SP_NEGATE {
		q.NextTok()
		n.tok1 = SP_NEGATE
	}
	switch q.Tok().id {
	case SP_LPAREN:
		tok := q.Tok()
		q.NextTok();
		n.node1,err = parseConditions(q)
		if err != nil { return n,err }
		tok = q.Tok()
		if tok.id != SP_RPAREN { return n,errors.New("No closing parentheses. Found: "+tok.val) }
		q.NextTok()
		n.node2,err = preparseMore(q)
		return n,err
	case WORD:
		//get column index before next step
		q.parseCol = COL_GETIDX
		_,err = columnParser(q)
		if err != nil { return n,err }
		q.lastColumn = TreeTok{0, q.parseCol, q.files["_fmk01"].types[q.parseCol]}
		//see if comparison is normal or between
		if q.Tok().id == KW_BETWEEN || q.PeekTok().id == KW_BETWEEN {
			n.node1, err = parseBetween(q)
		} else {
			n.node1, err = parseCompare(q)
		}
		if err != nil { return n,err }
		n.node2,err = preparseMore(q)
		return n,err
	}
	return n,errors.New("Unexpected token in 'where' section: "+q.Tok().val)
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
	if q.Tok().id == SP_NEGATE {
		q.NextTok()
		n.tok1 = SP_NEGATE
	}
	//relop and value
	if (q.Tok().id & RELOP) != 0 {
		tok := q.Tok()
		if tok.id == KW_LIKE { q.like = true; }
		n.tok2 = TreeTok{tok.id, tok.val, 0}
		q.NextTok()
		var err error
		n.tok3, err = comparisonValue(q)
		return n,err
	}
	return n,errors.New("Expected relational operator. Found: "+q.Tok().val)
}

//tok1 is logical operator
//node1 is next condition
func preparseMore(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_MORE}
	var err error
	if (q.Tok().id & LOGOP) == 0 { return n,nil }
	n.tok1 = q.Tok().id
	q.NextTok()
	n.node1,err = parseConditions(q)
	return n,err
}

//return parse tree segment for between as conditions node
func parseBetween(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_CONDITIONS}
	var  val1, val2, relop1, relop2 TreeTok
	var firstSmaller bool
	var err error
	//negation and get to value token
	if q.Tok().id == SP_NEGATE { n.tok1 = SP_NEGATE; q.NextTok() }
	q.NextTok()
	val1, err = comparisonValue(q)
	if err != nil { return n,err }
	if q.Tok().id != KW_AND { return n,errors.New("Expected 'and' in between clause, got "+q.Tok().val) }
	q.NextTok()
	val2, err = comparisonValue(q)
	if err != nil { return n,err }

	//see which is smaller
	if val1.dtype == T_NULL || val2.dtype == T_NULL { return n,errors.New("Cannot use 'null' with 'between'") }
	switch val1.dtype {
		case T_INT:	firstSmaller = val1.val.(int) < val2.val.(int)
		case T_FLOAT:  firstSmaller = val1.val.(float64) < val2.val.(float64)
		case T_DATE:   firstSmaller = val1.val.(time.Time).Before(val2.val.(time.Time))
		case T_STRING: firstSmaller = val1.val.(string) < val2.val.(string)
	}
	if firstSmaller {
		relop1 = TreeTok{SP_GREATEQ, ">=", 0}
		relop2 = TreeTok{SP_LESS, "<", 0}
	} else {
		relop1 = TreeTok{SP_LESS, "<", 0}
		relop2 = TreeTok{SP_GREATEQ, ">=", 0}
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
func comparisonValue(q* QuerySpecs) (TreeTok,error) {
	var tok TreeTok
	if q.Tok().id != WORD { return tok, errors.New("Expected a comparision value but got "+q.Tok().val) }
	_, val, err := dotParser(q, false)
	if err != nil { return tok, err }
	tok = TreeTok{0, val, q.lastColumn.dtype}
	//if relop is 'like', compile a regex
	if q.like {
		q.like = false
		re := regexp.MustCompile("%")
		tok.val = re.ReplaceAllString(Sprint(tok.val), ".*")
		re = regexp.MustCompile("_")
		tok.val = re.ReplaceAllString(Sprint(tok.val), ".")
		tok.val,err = regexp.Compile("(?i)^"+tok.val.(string)+"$")
		return tok, err
	}
	//otherwise parse its data type
	if s.ToLower(tok.val.(string)) == "null" { tok.dtype = T_NULL }
	switch tok.dtype {
		case T_INT:	tok.val,err = Atoi(tok.val.(string))
		case T_FLOAT:  tok.val,err = ParseFloat(tok.val.(string), 64)
		case T_DATE:   tok.val,err = d.ParseAny(tok.val.(string))
		case T_NULL:   tok.val = nil
		case T_STRING: tok.val = tok.val.(string)
	}
	return tok, err
}

//currently order is only thing after where
func parseOrder(q* QuerySpecs) error {
	var err error
	if q.Tok().id == EOS { return nil }
	if q.Tok().id == KW_ORDER {
		if q.NextTok().id != KW_BY { return errors.New("Expected 'by' after 'order'. Found "+q.Tok().val) }
		q.NextTok()
		q.parseCol = COL_GETIDX
		_,err = columnParser(q)
		if err == nil {
			q.sortCol = q.parseCol
			q.sortWay = 1
			if q.Tok().id == KW_ORDHOW { q.sortWay = 2; q.NextTok() }
		} else { return err }
	}
	return nil
}
