//data structures, constants, and whatnot
// _f# is file map key designed to avoid collisions with aliases and file names
package main
import (
	"regexp"
	"net/http"
	"github.com/gorilla/websocket"
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
	sortWay int
	save bool
	showLimit int
	tree *Node
	files map[string]*FileData
	numfiles int
	fromRow []string
	toRow []interface{}
	intColumn bool
	groupby bool
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
	N_FUNCTION = iota
	N_GROUPBY = iota
	N_EXPRESSIONS = iota
)
//tree node labels for debugging
var treeMap = map[int]string {
	N_QUERY:      "N_QUERY",
	N_SELECT:     "N_SELECT",
	N_SELECTIONS: "N_SELECTIONS",
	N_FROM:       "N_FROM",
	N_WHERE:      "N_WHERE",
	N_ORDER:      "N_ORDER",
	N_COLITEM:    "N_COLITEM",
	N_EXPRADD:    "N_EXPRADD",
	N_EXPRMULT:   "N_EXPRMULT",
	N_EXPRNEG:    "N_EXPRNEG",
	N_CPREDLIST:  "N_CPREDLIST",
	N_CPRED:      "N_CPRED",
	N_PREDICATES: "N_PREDICATES",
	N_PREDCOMP:   "N_PREDCOMP",
	N_CWEXPRLIST: "N_CWEXPRLIST",
	N_CWEXPR:     "N_CWEXPR",
	N_EXPRCASE:   "N_EXPRCASE",
	N_VALUE:      "N_VALUE",
	N_FUNCTION:   "N_FUNCTION",
	N_GROUPBY:    "N_GROUPBY",
	N_EXPRESSIONS:"N_EXPRESSIONS",
}
var typeMap = map[int]string {
	T_NULL:      "null",
	T_INT:       "integer",
	T_FLOAT:     "float",
	T_DATE:      "date",
	T_STRING:    "string",
}
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
	node4 *Node
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
			key := "_f" + Sprint(q.numfiles)
			q.files[key] = file
			q.files[filename[:len(filename)-4]] = file
			if q.NextTok().id == WORD { q.files[q.Tok().val] = file }
			if q.Tok().id == KW_AS && q.NextTok().id == WORD { q.files[q.Tok().val] = file }
			if err = inferTypes(q, key); err != nil {return err}
		}
	}
	q.Reset()
	if q.numfiles == 0 { return errors.New("Could not find file") }
	return nil
}

//channel data
const (
	CH_HEADER = iota
	CH_ROW = iota
	CH_DONE = iota
	CH_NEXT = iota
	CH_SAVPREP = iota
)
type saveData struct {
	Message string
	Number int
	Type int
	Header []string
	Row *[]interface{}
}
//one SingleQueryResult struct holds the results of one query
type SingleQueryResult struct {
	Numrows int
	ShowLimit int
	Numcols int
	Types []int
	Colnames []string
	Pos []int
	Vals [][]interface{}
	Status int
	Query string
}

//query return data struct and codes
const (
	DAT_ERROR = 1 << iota
	DAT_GOOD = 1 << iota
	DAT_BADPATH = 1 << iota
	DAT_IOERR = 1 << iota
	DAT_BLANK = 0
)
type ReturnData struct {
	Entries []SingleQueryResult
	Status int
	OriginalQuery string
	Clipped bool
	Message string
}

//file io struct and codes
const (
	FP_SERROR = 1 << iota
	FP_SCHANGED = 1 << iota
	FP_OERROR = 1 << iota
	FP_OCHANGED = 1 << iota
	FP_CWD = 0
	F_CSV = 1 << iota
	F_JSON = 1 << iota
	F_OPEN = 1 << iota
	F_SAVE = 1 << iota
)
type FilePaths struct {
	SavePath string
	OpenPath string
	Status int
}
//struct that matches incoming json requests
type Qrequest struct {
	Query string
	Qamount int
	FileIO int
	SavePath string
}

//websockets
const (
	SK_MSG = iota
	SK_PING = iota
	SK_PONG = iota
	SK_STOP = iota
	SK_DIRLIST = iota
	SK_FILECLICK = iota
)
type Client struct {
	conn *websocket.Conn
	w http.ResponseWriter
	r *http.Request
}
type sockMessage struct {
	Type int
	Text string
	Mode string
}
type sockDirMessage struct {
	Type int
	Dir Directory
}
