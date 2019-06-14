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
)

type QuerySpecs struct {
	colSpec Columns
	queryString string
	tokArray []Token
	tokIdx int
	quantityLimit int
	quantityRetrieved int
	distinctExpr *Node
	sortExpr *Node
	sortType int
	sortCol int
	sortWay int
	save bool
	like bool
	showLimit int
	tree *Node
	files map[string]*FileData
	numfiles int
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
	N_SELECTIONS = iota
	N_FROM = iota
	N_WHERE = iota
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
)
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
var countSelected int

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
	if q.numfiles == 0 { return errors.New("Could not find file") }
	return nil
}
