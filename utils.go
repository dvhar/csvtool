//data structures, constants, helper functions, and whatnot. Should really clean this up
// _f# is file map key designed to avoid collisions with aliases and file names
package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	. "fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	. "strconv"
	"strings"
	"time"

	d "github.com/araddon/dateparse"
	bt "github.com/google/btree"
	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh/terminal"
)

type QuerySpecs struct {
	colSpec           Columns
	QueryString       string
	tokArray          []Token
	aliases           bool
	joining           bool
	tokIdx            int
	quantityLimit     int
	quantityRetrieved int
	distinctExpr      *Node
	distinctCheck     *bt.BTree
	sortExpr          *Node
	sortWay           int
	save              bool
	showLimit         int
	stage             int
	tree              *Node
	files             map[string]*FileData
	numfiles          int
	fromRow           []string
	toRow             []Value
	midRow            []Value
	midExess          int
	intColumn         bool
	distinctAgg       bool
	groupby           bool
	noheader          bool
	bigjoin           bool
	joinSortVals      []J2ValPos
	gettingSortVals   bool
	password          string
}

func (q *QuerySpecs) NextTok() *Token {
	if q.tokIdx < len(q.tokArray)-1 {
		q.tokIdx++
	}
	return &q.tokArray[q.tokIdx]
}
func (q QuerySpecs) PeekTok() *Token {
	if q.tokIdx < len(q.tokArray)-1 {
		return &q.tokArray[q.tokIdx+1]
	}
	return &q.tokArray[q.tokIdx]
}
func (q QuerySpecs) Tok() *Token { return &q.tokArray[q.tokIdx] }
func (q *QuerySpecs) Reset()     { q.tokIdx = 0 }
func (q QuerySpecs) LimitReached() bool {
	return q.quantityRetrieved >= q.quantityLimit && q.quantityLimit > 0
}
func (q *QuerySpecs) SaveJoinPos(val Value) {
	var j2 int64
	if q.bigjoin {
		j2 = q.files["_f2"].reader.prevPos
	} else {
		j2 = q.files["_f2"].reader.index
	}
	q.joinSortVals = append(q.joinSortVals, J2ValPos{q.files["_f1"].reader.prevPos, j2, val})
}

func intInList(x int, i ...int) bool {
	for _, j := range i {
		if x == j {
			return true
		}
	}
	return false
}

//Random access csv reader
type LineReader struct {
	valPositions []ValPos
	lineBytes    []byte
	fromRow      []string
	index        int64
	maxLineSize  int
	pos          int64
	prevPos      int64
	lineBuffer   bytes.Buffer
	tee          io.Reader
	csvReader    *csv.Reader
	byteReader   *bytes.Reader
	fp           *os.File
}

func (l *LineReader) GetPos() int64 { return l.prevPos }
func (l *LineReader) SavePos(value Value) {
	l.valPositions = append(l.valPositions, ValPos{l.prevPos, value})
}
func (l *LineReader) SavePosTo(value Value, arr *[]ValPos) {
	*arr = append(*arr, ValPos{l.prevPos, value})
}
func (l *LineReader) PrepareReRead() {
	l.lineBytes = make([]byte, l.maxLineSize)
	l.byteReader = bytes.NewReader(l.lineBytes)
}
func (l *LineReader) Init(q *QuerySpecs, f string) {
	l.fp, _ = os.Open(q.files[f].fname)
	l.valPositions = make([]ValPos, 0)
	l.tee = io.TeeReader(l.fp, &l.lineBuffer)
	l.csvReader = csv.NewReader(l.tee)
	if !q.noheader {
		l.Read()
	}
}

func (l *LineReader) Read() ([]string, error) {
	var err error
	l.fromRow, err = l.csvReader.Read()
	l.lineBytes, _ = l.lineBuffer.ReadBytes('\n')
	size := len(l.lineBytes)
	if l.maxLineSize < size {
		l.maxLineSize = size
	}
	l.prevPos = l.pos
	l.pos += int64(size)
	return l.fromRow, err
}
func (l *LineReader) ReadAtIndex(lineNo int) ([]string, error) {
	return l.ReadAtPosition(l.valPositions[lineNo].pos)
}
func (l *LineReader) ReadAtPosition(pos int64) ([]string, error) {
	l.prevPos = pos
	l.fp.ReadAt(l.lineBytes, pos)
	l.byteReader.Seek(0, 0)
	l.csvReader = csv.NewReader(l.byteReader)
	var err error
	l.fromRow, err = l.csvReader.Read()
	return l.fromRow, err
}

//return error, return type of certain functions
func checkFunctionParamType(functionId, typ int) (int, error) {
	err := func(s string) error { return errors.New(s + ", not " + typeMap[typ]) }
	switch functionId {
	case FN_STDEVP:
		fallthrough
	case FN_STDEV:
		if !intInList(typ, T_FLOAT, T_INT) {
			return typ, err("can only find standard deviation of numbers")
		}
		typ = T_FLOAT
	case FN_SUM:
		if !intInList(typ, T_INT, T_FLOAT, T_DURATION) {
			return typ, err("can only sum numbers")
		}
	case FN_AVG:
		if !intInList(typ, T_INT, T_FLOAT, T_DURATION) {
			return typ, err("can only average numbers")
		}
		if typ == T_INT {
			typ = T_FLOAT
		}
	case FN_ABS:
		if !intInList(typ, T_INT, T_FLOAT, T_DURATION) {
			return typ, err("can only find absolute value of numbers")
		}
	case FN_YEAR:
		if !intInList(typ, T_DATE) {
			return typ, err("can only find year of date type")
		}
		typ = T_INT
	case FN_MONTHNAME:
		if !intInList(typ, T_DATE) {
			return typ, err("can only find month of date type")
		}
		typ = T_STRING
	case FN_MONTH:
		if !intInList(typ, T_DATE) {
			return typ, err("can only find month of date type")
		}
		typ = T_INT
	case FN_WEEK:
		if !intInList(typ, T_DATE) {
			return typ, err("can only find week of date type")
		}
		typ = T_INT
	case FN_WDAYNAME:
		if !intInList(typ, T_DATE) {
			return typ, err("can only find day of date type")
		}
		typ = T_STRING
	case FN_WDAY:
		fallthrough
	case FN_YDAY:
		fallthrough
	case FN_MDAY:
		if !intInList(typ, T_DATE) {
			return typ, err("can only find day of date type")
		}
		typ = T_INT
	case FN_HOUR:
		if !intInList(typ, T_DATE) {
			return typ, err("can only find hour of date/time type")
		}
		typ = T_INT
	}
	return typ, nil
}

func checkOperatorSemantics(operator, t1, t2 int, l1, l2 bool) error {
	err := func(s string) error { return errors.New(s) }
	switch operator {
	case SP_PLUS:
		if t1 == T_DATE && t2 == T_DATE {
			return err("Cannot add 2 dates")
		}
		fallthrough
	case SP_MINUS:
		if !isOneOfType(t1, t2, T_INT, T_FLOAT) && !(typeCompute(l1, l2, t1, t2) == T_STRING) &&
			!((t1 == T_DATE && t2 == T_DURATION) || (t1 == T_DURATION && t2 == T_DATE) ||
				(t1 == T_DATE && t2 == T_DATE) || t1 == T_DURATION && t2 == T_DURATION) {
			return err("Cannot add or subtract types " + typeMap[t1] + " and " + typeMap[t2])
		}
	case SP_MOD:
		if t1 != T_INT || t2 != T_INT {
			return err("Modulus operator requires integers")
		}
	case SP_DIV:
		if t1 == T_INT && t2 == T_DURATION {
			return err("Cannot divide integer by time duration")
		}
		fallthrough
	case SP_STAR:
		if !isOneOfType(t1, t2, T_INT, T_FLOAT) &&
			!((t1 == T_INT && t2 == T_DURATION) || (t1 == T_DURATION && t2 == T_INT)) &&
			!((t1 == T_FLOAT && t2 == T_DURATION) || (t1 == T_DURATION && t2 == T_FLOAT)) {
			return err("Cannot multiply or divide types " + typeMap[t1] + " and " + typeMap[t2])
		}
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
var treeMap = map[int]string{
	N_QUERY:        "N_QUERY",
	N_SELECT:       "N_SELECT",
	N_SELECTIONS:   "N_SELECTIONS",
	N_FROM:         "N_FROM",
	N_WHERE:        "N_WHERE",
	N_ORDER:        "N_ORDER",
	N_EXPRADD:      "N_EXPRADD",
	N_EXPRMULT:     "N_EXPRMULT",
	N_EXPRNEG:      "N_EXPRNEG",
	N_CPREDLIST:    "N_CPREDLIST",
	N_CPRED:        "N_CPRED",
	N_PREDICATES:   "N_PREDICATES",
	N_PREDCOMP:     "N_PREDCOMP",
	N_CWEXPRLIST:   "N_CWEXPRLIST",
	N_CWEXPR:       "N_CWEXPR",
	N_EXPRCASE:     "N_EXPRCASE",
	N_VALUE:        "N_VALUE",
	N_FUNCTION:     "N_FUNCTION",
	N_GROUPBY:      "N_GROUPBY",
	N_EXPRESSIONS:  "N_EXPRESSIONS",
	N_JOINCHAIN:    "N_JOINCHAIN",
	N_JOIN:         "N_JOIN",
	N_DEXPRESSIONS: "N_DEXPRESSIONS",
}
var typeMap = map[int]string{
	T_NULL:     "null",
	T_INT:      "integer",
	T_FLOAT:    "float",
	T_DATE:     "date",
	T_DURATION: "duration",
	T_STRING:   "string",
}

type FileData struct {
	fname    string
	names    []string
	types    []int
	width    int
	key      string
	id       int
	noheader bool
	reader   *LineReader
	fromRow  []string
}
type Node struct {
	label int
	tok1  interface{}
	tok2  interface{}
	tok3  interface{}
	tok4  interface{}
	tok5  interface{}
	node1 *Node
	node2 *Node
	node3 *Node
	node4 *Node
	node5 *Node
}
type Columns struct {
	NewNames       []string
	NewTypes       []int
	NewPos         []int
	NewWidth       int
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
	if a > b {
		return a
	}
	return b
}
func getColumnIdx(colNames []string, column string) (int, error) {
	for i, col := range colNames {
		if strings.ToLower(col) == strings.ToLower(column) {
			return i, nil
		}
	}
	return 0, errors.New("Column " + column + " not found")
}

var countSelected int

//infer type of single string value
var LeadingZeroString *regexp.Regexp = regexp.MustCompile(`^0\d+$`)

func getNarrowestType(value string, startType int) int {
	entry := strings.TrimSpace(value)
	if strings.ToLower(entry) == "null" || entry == "NA" || entry == "" {
		startType = max(T_NULL, startType)
	} else if LeadingZeroString.MatchString(entry) {
		startType = T_STRING
	} else if _, err := Atoi(entry); err == nil {
		startType = max(T_INT, startType)
	} else if _, err := ParseFloat(entry, 64); err == nil {
		startType = max(T_FLOAT, startType)
	} else if _, err := d.ParseAny(entry); err == nil {
		startType = max(T_DATE, startType)
		//in case duration gets mistaken for a date
		if _, err := parseDuration(entry); err == nil {
			startType = max(T_DURATION, startType)
		}
	} else if _, err := parseDuration(entry); err == nil {
		startType = max(T_DURATION, startType)
	} else {
		startType = T_STRING
	}
	return startType
}

//infer types of all infile columns
func inferTypes(q *QuerySpecs, key string) error {
	//open file
	fp, err := os.Open(q.files[key].fname)
	if err != nil {
		return errors.New("problem opening input file")
	}
	defer func() { fp.Seek(0, 0); fp.Close() }()
	cread := csv.NewReader(fp)
	line, err := cread.Read()
	if err != nil {
		return errors.New("problem reading input file")
	}
	//get col names and initialize blank types
	for i, entry := range line {
		if q.noheader || q.files[key].noheader {
			q.files[key].names = append(q.files[key].names, Sprintf("col%d", i+1))
		} else {
			q.files[key].names = append(q.files[key].names, entry)
		}
		q.files[key].types = append(q.files[key].types, 0)
		q.files[key].width = i + 1
	}
	//get samples and infer types from them
	if !q.noheader && !q.files[key].noheader {
		line, err = cread.Read()
	}
	var e error
	for j := 0; j < 10000; j++ {
		for i, cell := range line {
			q.files[key].types[i] = getNarrowestType(cell, q.files[key].types[i])
		}
		line, e = cread.Read()
		if e != nil {
			break
		}
	}
	return err
}

var durationPattern *regexp.Regexp = regexp.MustCompile(`^(\d+|\d+\.\d+)\s(seconds|second|minutes|minute|hours|hour|days|day|weeks|week|years|year|s|m|h|d|w|y)$`)

func parseDuration(str string) (time.Duration, error) {
	dur, err := time.ParseDuration(str)
	if err == nil {
		return dur, err
	}
	if !durationPattern.MatchString(str) {
		return 0, errors.New("Error: Could not parse '" + str + "' as a time duration")
	}
	times := strings.Split(str, " ")
	quantity, _ := ParseFloat(times[0], 64)
	unit := times[1]
	switch unit {
	case "y":
		fallthrough
	case "year":
		fallthrough
	case "years":
		quantity *= 52
		fallthrough
	case "w":
		fallthrough
	case "week":
		fallthrough
	case "weeks":
		quantity *= 7
		fallthrough
	case "d":
		fallthrough
	case "day":
		fallthrough
	case "days":
		quantity *= 24
		fallthrough
	case "h":
		fallthrough
	case "hour":
		fallthrough
	case "hours":
		quantity *= 60
		fallthrough
	case "m":
		fallthrough
	case "minute":
		fallthrough
	case "minutes":
		quantity *= 60
		fallthrough
	case "s":
		fallthrough
	case "second":
		fallthrough
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
	parsingOptions := true
	for ; q.Tok().id != EOS; q.NextTok() {
		//parse options here because they can effect file prep
		if parsingOptions {
			switch q.Tok().Lower() {
			case "c":
				q.intColumn = true
			case "header":
			case "h":
			case "noheader":
				fallthrough
			case "nh":
				q.noheader = true
			case "select":
				parsingOptions = false
			}
		}
		tok := strings.Replace(q.Tok().val, "~/", os.Getenv("HOME")+"/", 1)
		_, err := os.Stat(tok)
		//open file and add to file map
		if err == nil && extension.MatchString(tok) {
			q.numfiles++
			file := &FileData{fname: tok, id: q.numfiles}
			filename := filepath.Base(file.fname)
			key := "_f" + Sprint(q.numfiles)
			file.key = key
			q.files[key] = file
			q.files[filename[:len(filename)-4]] = file
			if q.PeekTok().Lower() == "noheader" || q.PeekTok().Lower() == "nh" {
				q.NextTok()
				q.files[key].noheader = true
			}
			if _, ok := joinMap[q.PeekTok().Lower()]; !ok && q.NextTok().id == WORD {
				q.files[q.Tok().val] = file
				q.aliases = true
			}
			if _, ok := joinMap[q.PeekTok().Lower()]; !ok && q.Tok().Lower() == "as" && q.NextTok().id == WORD {
				q.files[q.Tok().val] = file
				q.aliases = true
			}
			if q.PeekTok().Lower() == "noheader" || q.PeekTok().Lower() == "nh" {
				q.files[key].noheader = true
			}
			if err = inferTypes(q, key); err != nil {
				return err
			}
			var reader LineReader
			globalNoHeader := q.noheader
			q.noheader = q.files[key].noheader
			q.files[key].reader = &reader
			q.files[key].reader.Init(q, key)
			q.noheader = globalNoHeader
		}
	}
	q.Reset()
	if q.numfiles == 0 {
		return errors.New("Could not find file")
	}
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
	Number  int
	Type    int
	Header  []string
	Row     *[]Value
}

//one SingleQueryResult struct holds the results of one query
type SingleQueryResult struct {
	Numrows   int
	ShowLimit int
	Numcols   int
	Types     []int
	Colnames  []string
	Pos       []int
	Vals      [][]Value
	Status    int
	Query     string
}

//query return data struct and codes
const (
	DAT_ERROR   = 1 << iota
	DAT_GOOD    = 1 << iota
	DAT_BADPATH = 1 << iota
	DAT_IOERR   = 1 << iota
	DAT_BLANK   = 0
)

type ReturnData struct {
	Entries       []SingleQueryResult
	Status        int
	OriginalQuery string
	Clipped       bool
	Message       string
}

//file io struct and codes
const (
	FP_SERROR   = 1 << iota
	FP_SCHANGED = 1 << iota
	FP_OERROR   = 1 << iota
	FP_OCHANGED = 1 << iota
	FP_CWD      = 0
	F_CSV       = 1 << iota
	F_JSON      = 1 << iota
	F_OPEN      = 1 << iota
	F_SAVE      = 1 << iota
)

type FilePaths struct {
	SavePath string
	OpenPath string
	Status   int
}

//struct that matches incoming json requests
type webQueryRequest struct {
	Query    string
	Qamount  int
	FileIO   int
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
	SK_PASS
)

type Client struct {
	conn *websocket.Conn
	w    http.ResponseWriter
	r    *http.Request
}
type sockMessage struct {
	Type int
	Text string
	Mode string
}
type sockDirMessage struct {
	Type int
	Dir  Directory
}
type Flags struct {
	localPort  *string
	danger     *bool
	persistent *bool
	command    *string
	version    *bool
}

var Testing bool

func (f Flags) gui() bool {
	if f.command == nil {
		return false
	}
	return *f.command == "" && Testing == false
}

func message(s string) {
	if flags.gui() {
		go func() { messager <- s }()
	} else {
		print("\r" + s)
	}
}

func eosError(q *QuerySpecs) error {
	if q.PeekTok().id == 255 {
		return errors.New("Unexpected end of query string")
	}
	return nil
}
func ErrMsg(t *Token, s string) error {
	return errors.New(Sprint("Line ", t.line, " Col ", t.col, ": ", s))
}

type J2ValPos struct {
	pos1 int64
	pos2 int64
	val  Value
}
type ValPos struct {
	pos int64
	val Value
}
type ValRow struct {
	row []string
	val Value
	idx int
}

type JoinFinder struct {
	jfile    string
	joinNode *Node
	baseNode *Node
	posArr   []ValPos //store file position for big file
	rowArr   []ValRow //store whole rows for small file
	i        int
}

//find matching file position for big files
func (jf *JoinFinder) FindNextBig(val Value) int64 {
	//use binary search for leftmost instance
	r := len(jf.posArr) - 1
	if jf.i == -1 {
		l := 0
		var m int
		for l < r {
			m = (l + r) / 2
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
func (jf *JoinFinder) FindNextSmall(val Value) *ValRow {
	//use binary search for leftmost instance
	r := len(jf.rowArr) - 1
	if jf.i == -1 {
		l := 0
		var m int
		for l < r {
			m = (l + r) / 2
			if jf.rowArr[m].val.Less(val) {
				l = m + 1
			} else {
				r = m
			}
		}
		if jf.rowArr[l].val.Equal(val) {
			jf.i = l
			return &jf.rowArr[l]
		}
		//check right neighbors for more matches
	} else {
		if jf.i < r-1 {
			jf.i++
			if jf.rowArr[jf.i].val.Equal(val) {
				return &jf.rowArr[jf.i]
			}
		}
	}
	jf.i = -1 //set i to -1 when done so next search is binary
	return nil
}
func (jf *JoinFinder) Sort() {
	sort.Slice(jf.posArr, func(i, j int) bool { return jf.posArr[j].val.Greater(jf.posArr[i].val) })
	sort.Slice(jf.rowArr, func(i, j int) bool { return jf.rowArr[j].val.Greater(jf.rowArr[i].val) })
	jf.i = -1
}

//call returned func with (true) to start and (false) to stop
var START bool = true
var STOP bool = false

func TimedNotifier(S ...interface{}) func(bool) {
	var stop bool
	return func(active bool) {
		if active {
			go func() {
				ticker := time.NewTicker(time.Second)
				for {
					<-ticker.C
					if stop {
						return
					}
					m := ""
					for _, v := range S {
						switch vv := v.(type) {
						case string:
							m += vv
						case *string:
							m += *vv
						case int:
							m += Itoa(vv)
						case *int:
							m += Itoa(*vv)
						}
					}
					message(m)
				}
			}()
		} else {
			stop = true
		}
	}
}
func promptPassword() string {
	if flags.gui() {
		passprompt <- true
		println("sent passprompt socket")
		password := <-gotpass
		println("recieved pass")
		return password
	}
	println("Enter password for encryption function:")
	passbytes, _ := terminal.ReadPassword(0)
	return string(passbytes)
}
