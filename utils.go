//data structures, constants, helper functions, and whatnot. Should really clean this up
// _f# is file map key designed to avoid collisions with aliases and file names
package main
import (
	"regexp"
	"net/http"
	"sort"
	"github.com/gorilla/websocket"
	"encoding/csv"
	"path/filepath"
	"os"
	"errors"
	"time"
	s "strings"
	d "github.com/araddon/dateparse"
	bt "github.com/google/btree"
	. "strconv"
	. "fmt"
)

type QuerySpecs struct {
	colSpec Columns
	QueryString string
	tokArray []Token
	aliases bool
	joining bool
	tokIdx int
	quantityLimit int
	quantityRetrieved int
	distinctExpr *Node
	distinctCheck *bt.BTree
	sortExpr *Node
	sortWay int
	save bool
	showLimit int
	stage int
	tree *Node
	files map[string]*FileData
	numfiles int
	fromRow []string
	toRow []Value
	midRow []Value
	midExess int
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
func (q QuerySpecs) LimitReached() bool { return q.quantityRetrieved >= q.quantityLimit && q.quantityLimit > 0 }

func intInList(x int, i ...int) bool {
	for _,j := range i { if x == j { return true } }
	return false
}

func checkFunctionParamType(functionId, typ int) error {
	err := func(s string) error { return errors.New(s+", not "+typeMap[typ]) }
	switch functionId {
	case FN_SUM:   if !intInList(typ, T_INT, T_FLOAT, T_DURATION) { return err("can only sum numbers") }
	case FN_AVG:   if !intInList(typ, T_INT, T_FLOAT, T_DURATION) { return err("can only average numbers") }
	case FN_ABS:   if !intInList(typ, T_INT, T_FLOAT, T_DURATION) { return err("can only find absolute value of numbers") }
	case FN_YEAR:  if !intInList(typ, T_DATE) { return err("can only find year of date type") }
	case FN_MONTH: if !intInList(typ, T_DATE) { return err("can only find month of date type") }
	case FN_WEEK:  if !intInList(typ, T_DATE) { return err("can only find week of date type") }
	case FN_WDAY:   if !intInList(typ, T_DATE) { return err("can only find day of date type") }
	case FN_HOUR:  if !intInList(typ, T_DATE) { return err("can only find hour of date/time type") }
	}
	return nil
}

func checkOperatorSemantics(operator, t1, t2 int, l1, l2 bool) error {
	err := func(s string) error { return errors.New(s) }
	switch operator {
	case SP_PLUS:
		if  t1 == T_DATE && t2 == T_DATE { return err("Cannot add 2 dates") }
		fallthrough
	case SP_MINUS:
		if !isOneOfType(t1,t2,T_INT,T_FLOAT) && !(typeCompute(l1,l2,t1,t2)==T_STRING) &&
			!((t1 == T_DATE && t2 == T_DURATION) || (t1 == T_DURATION && t2 == T_DATE) ||
			(t1 == T_DATE && t2 == T_DATE) || t1 == T_DURATION && t2 == T_DURATION) {
			return err("Cannot add or subtract types "+typeMap[t1]+" and "+typeMap[t2])
	}
	case SP_MOD: if (t1!=T_INT || t2!=T_INT) { return err("Modulus operator requires integers") }
	case SP_DIV:
		if t1 == T_INT && t2 == T_DURATION { return err("Cannot divide integer by time duration") }
		fallthrough
	case SP_STAR:
		if !isOneOfType(t1,t2,T_INT,T_FLOAT) &&
			!((t1 == T_INT && t2 == T_DURATION) || (t1 == T_DURATION && t2 == T_INT)) &&
			!((t1 == T_FLOAT && t2 == T_DURATION) || (t1 == T_DURATION && t2 == T_FLOAT)){
			return err("Cannot multiply or divide types "+typeMap[t1]+" and "+typeMap[t2]) }
	}
	return nil
}

const (
	//parse tree node types
	N_QUERY = iota
	N_SELECT
	N_SELECTIONS
	N_FROM
	N_JOINCHAIN
	N_JOIN
	N_WHERE
	N_ORDER
	N_EXPRADD
	N_EXPRMULT
	N_EXPRNEG
	N_EXPRCASE
	N_CPREDLIST
	N_CPRED
	N_CWEXPRLIST
	N_CWEXPR
	N_PREDICATES
	N_PREDCOMP
	N_VALUE
	N_FUNCTION
	N_GROUPBY
	N_EXPRESSIONS
	N_DEXPRESSIONS
)
//tree node labels for debugging
var treeMap = map[int]string {
	N_QUERY:      "N_QUERY",
	N_SELECT:     "N_SELECT",
	N_SELECTIONS: "N_SELECTIONS",
	N_FROM:       "N_FROM",
	N_WHERE:      "N_WHERE",
	N_ORDER:      "N_ORDER",
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
	N_JOINCHAIN:  "N_JOINCHAIN",
	N_JOIN:       "N_JOIN",
	N_DEXPRESSIONS:"N_DEXPRESSIONS",
}
var typeMap = map[int]string {
	T_NULL:      "null",
	T_INT:       "integer",
	T_FLOAT:     "float",
	T_DATE:      "date",
	T_DURATION:  "duration",
	T_STRING:    "string",
}
type FileData struct {
	fname string
	names []string
	types []int
	width int
	key string
	id int
	reader *LineReader
	fromRow []string
}
type Node struct {
	label int
	tok1 interface{}
	tok2 interface{}
	tok3 interface{}
	tok4 interface{}
	tok5 interface{}
	node1 *Node
	node2 *Node
	node3 *Node
	node4 *Node
	node5 *Node
}
type Columns struct {
	NewNames []string
	NewTypes []int
	NewPos []int
	NewWidth int
	AggregateCount int
}
const (
	T_NULL = iota
	T_INT
	T_FLOAT
	T_DATE
	T_DURATION
	T_STRING
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
var LeadingZeroString *regexp.Regexp = regexp.MustCompile(`^0\d+$`)
func getNarrowestType(value string, startType int) int {
	entry := s.TrimSpace(value)
	if s.ToLower(entry) == "null" || entry == "NA" || entry == "" {
	  startType = max(T_NULL, startType)
	} else if LeadingZeroString.MatchString(entry)       { startType = T_STRING
	} else if _, err := Atoi(entry); err == nil          { startType = max(T_INT, startType)
	} else if _, err := ParseFloat(entry,64); err == nil { startType = max(T_FLOAT, startType)
	} else if _,err := d.ParseAny(entry); err == nil     { startType = max(T_DATE, startType)
	  //in case duration gets mistaken for a date
	   if _,err := parseDuration(entry); err == nil      { startType = max(T_DURATION, startType)}
	} else if _,err := parseDuration(entry); err == nil  { startType = max(T_DURATION, startType)
	} else                                               { startType = T_STRING }
	return startType
}
//infer types of all infile columns
func inferTypes(q *QuerySpecs, f string) error {
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
var durationPattern *regexp.Regexp = regexp.MustCompile(`^(\d+|\d+\.\d+)\s(seconds|second|minutes|minute|hours|hour|days|day|weeks|week|years|year|s|m|h|d|w|y)$`)
func parseDuration(str string) (time.Duration, error) {
	dur, err := time.ParseDuration(str)
	if err == nil { return dur, err }
	if !durationPattern.MatchString(str) { return 0, errors.New("Error: Could not parse '"+str+"' as a time duration") }
	times := s.Split(str," ")
	quantity,_ := ParseFloat(times[0],64)
	unit := times[1]
	switch unit {
		case "y":    fallthrough
		case "year": fallthrough
		case "years":
			quantity *= 52
			fallthrough
		case "w":    fallthrough
		case "week": fallthrough
		case "weeks":
			quantity *= 7
			fallthrough
		case "d":   fallthrough
		case "day": fallthrough
		case "days":
			quantity *= 24
			fallthrough
		case "h":    fallthrough
		case "hour": fallthrough
		case "hours":
			quantity *= 60
			fallthrough
		case "m":      fallthrough
		case "minute": fallthrough
		case "minutes":
			quantity *= 60
			fallthrough
		case "s":      fallthrough
		case "second": fallthrough
		case "seconds":
			return time.Second * time.Duration(quantity), nil
	}
	//find a way to implement month math
	return 0, errors.New("Error: Unable to calculate months")
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
			q.numfiles++
			file := &FileData{fname : q.Tok().val, id : q.numfiles}
			filename := filepath.Base(file.fname)
			key := "_f" + Sprint(q.numfiles)
			file.key = key
			q.files[key] = file
			q.files[filename[:len(filename)-4]] = file
			if _,ok:=joinMap[q.PeekTok().Lower()];!ok && q.NextTok().id==WORD {
				q.files[q.Tok().val] = file
				q.aliases = true
			}
			if _,ok:=joinMap[q.PeekTok().Lower()];!ok && q.Tok().Lower()=="as" && q.NextTok().id==WORD {
				q.files[q.Tok().val] = file
				q.aliases = true
			}
			if err = inferTypes(q, key); err != nil {return err}
			var reader LineReader
			q.files[key].reader = &reader
			q.files[key].reader.Init(q,key)
		}
	}
	q.Reset()
	if q.numfiles == 0 { return errors.New("Could not find file") }
	return nil
}

//channel data
const (
	CH_HEADER = iota
	CH_ROW
	CH_DONE
	CH_NEXT
	CH_SAVPREP
)
type saveData struct {
	Message string
	Number int
	Type int
	Header []string
	Row *[]Value
}
//one SingleQueryResult struct holds the results of one query
type SingleQueryResult struct {
	Numrows int
	ShowLimit int
	Numcols int
	Types []int
	Colnames []string
	Pos []int
	Vals [][]Value
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
type webQueryRequest struct {
	Query string
	Qamount int
	FileIO int
	SavePath string
}

//websockets
const (
	SK_MSG = iota
	SK_PING
	SK_PONG
	SK_STOP
	SK_DIRLIST
	SK_FILECLICK
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
type Flags struct {
	localPort *string
	danger *bool
	persistent *bool
	command *string
}
var Testing bool
func (f Flags) gui() bool {
    if f.command == nil { return false }
    return *f.command == "" && Testing == false
}

func message(s string) {
	if flags.gui() {
		go func(){ messager <- s }()
	} else {
		print("\r"+s)
	}
}

func eosError(q *QuerySpecs) error {
	if q.PeekTok().id == 255 { return errors.New("Unexpected end of query string") }
	return nil
}

type ValPos struct {
	pos int64
	val Value
}
type ValRow struct {
	row []string
	val Value
}

type JoinFinder struct {
	jfile string
	joinNode *Node
	baseNode *Node
	posArr []ValPos //store file position for big file
	rowArr []ValRow //store whole rows for small file
	i int
}
//find matching file position for big files
func (jf *JoinFinder) FindNextBig(val Value) int64 {
	//use binary search for leftmost instance
	r := len(jf.posArr)-1
	if jf.i == -1 {
		l := 0
		var m int
		for ;l<r; {
			m = (l+r)/2
			if jf.posArr[m].val.Less(val) {
				l = m + 1
			} else {
				r = m
			}
		}
		if jf.posArr[l].val.Equal(val) {
			jf.i = l
			return jf.posArr[l].pos
		}
	//check right neighbors for more matches
	} else {
		if jf.i < r-1 {
			jf.i++
			if jf.posArr[jf.i].val.Equal(val) {
				return jf.posArr[jf.i].pos
			}
		}
	}
	jf.i = -1 //set i to -1 when done so next search is binary
	return -1
}
//find matching row in memory for small files
func (jf *JoinFinder) FindNextSmall(val Value) ([]string,error) {
	//use binary search for leftmost instance
	r := len(jf.rowArr)-1
	if jf.i == -1 {
		l := 0
		var m int
		for ;l<r; {
			m = (l+r)/2
			if jf.rowArr[m].val.Less(val) {
				l = m + 1
			} else {
				r = m
			}
		}
		if jf.rowArr[l].val.Equal(val) {
			jf.i = l
			return jf.rowArr[l].row, nil
		}
	//check right neighbors for more matches
	} else {
		if jf.i < r-1 {
			jf.i++
			if jf.rowArr[jf.i].val.Equal(val) {
				return jf.rowArr[jf.i].row, nil
			}
		}
	}
	jf.i = -1 //set i to -1 when done so next search is binary
	return nil, errors.New("none")
}
func (jf *JoinFinder) Sort() {
	sort.Slice(jf.posArr, func(i, j int) bool { return jf.posArr[j].val.Greater(jf.posArr[i].val) })
	sort.Slice(jf.rowArr, func(i, j int) bool { return jf.rowArr[j].val.Greater(jf.rowArr[i].val) })
	jf.i = -1
}
