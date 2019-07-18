//data structures, constants, helper functions, and whatnot. Should really clean this up
// _f# is file map key designed to avoid collisions with aliases and file names
package main
import (
	"regexp"
	"net/http"
	"github.com/gorilla/websocket"
	"encoding/csv"
	"encoding/json"
	"path/filepath"
	"os"
	"time"
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

func checkOperatorSemantics(operator, t1, t2 int, v1, v2 interface{}) error {
	err := func(s string) error { return errors.New(s) }
	switch operator {
	case SP_PLUS:
		if  t1 == T_DATE && t2 == T_DATE { return err("Cannot add 2 dates") }
		fallthrough
	case SP_MINUS:
		if !isOneOfType(t1,t2,T_INT,T_FLOAT) && !(typeCompute(v1,v2,t1,t2)==T_STRING) &&
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

//interface to simplify operations with various datatypes
type Value interface {
	Greater(other Value) bool
	GreatEq(other Value) bool
	Less(other Value) bool
	LessEq(other Value) bool
	Equal(other Value) bool
	Add(other Value) Value
	Sub(other Value) Value
	Mult(other Value) Value
	Div(other Value) Value
	Mod(other Value) Value
	String() string
	MarshalJSON() ([]byte,error)
}

type AverageVal struct {
	val Value
	count integer
}
func (a AverageVal) Add(other Value) Value       { return AverageVal{ a.val.Add(other), a.count + 1, } }
func (a AverageVal) String() string              { return a.Eval().String() }
func (a AverageVal) MarshalJSON() ([]byte,error) { return json.Marshal(a.String()) }
func (a AverageVal) Greater(other Value) bool    { return false }
func (a AverageVal) GreatEq(other Value) bool    { return false }
func (a AverageVal) Less(other Value) bool       { return false }
func (a AverageVal) LessEq(other Value) bool     { return false }
func (a AverageVal) Equal(other Value) bool      { return false }
func (a AverageVal) Sub(other Value) Value       { return a }
func (a AverageVal) Mult(other Value) Value      { return a }
func (a AverageVal) Div(other Value) Value       { return a }
func (a AverageVal) Mod(other Value) Value       { return a }
func (a AverageVal) Eval() Value                 { return a.val.Div(a.count) }

type float float64
type integer int
type date struct {val time.Time}
type duration struct {val time.Duration}
type text string
type null string
type liker struct {val *regexp.Regexp}

func (f float) Less(other Value) bool      {
	switch o := other.(type) {
		case float:   return f < o
		case integer: return f < float(o)
	}
	return false
}
func (i integer) Less(other Value) bool    { return i < other.(integer) }
func (d date) Less(other Value) bool       { return d.val.Before(other.(date).val) }
func (d duration) Less(other Value) bool   {
	switch o := other.(type) {
		case duration: return d.val < o.val
		case integer:  return d.val < time.Duration(o)
	}
	return false
}
func (t text) Less(other Value) bool       { if _,ok:=other.(text);!ok {return false};return t < other.(text) }
func (n null) Less(other Value) bool       { if _,ok:=other.(null);ok  {return false};return true }
func (l liker) Less(other Value) bool      { return false }

func (f float) LessEq(other Value) bool    { if _,ok:=other.(float);!ok    {return false};return f <= other.(float) }
func (i integer) LessEq(other Value) bool  { if _,ok:=other.(integer);!ok  {return false};return i <= other.(integer) }
func (d date) LessEq(other Value) bool     { if _,ok:=other.(duration);!ok {return false};return !d.val.After(other.(date).val) }
func (d duration) LessEq(other Value) bool { if _,ok:=other.(date);!ok     {return false};return d.val <= other.(duration).val }
func (t text) LessEq(other Value) bool     { if _,ok:=other.(text);!ok     {return false};return t <= other.(text) }
func (n null) LessEq(other Value) bool     { return false }
func (l liker) LessEq(other Value) bool    { return false }

func (f float) Greater(other Value) bool   { if _,ok := other.(float); !ok    { return true } else {return f > other.(float) } }
func (i integer) Greater(other Value) bool { if _,ok := other.(integer); !ok  { return true } else {return i > other.(integer) } }
func (d date) Greater(other Value) bool    { if _,ok := other.(date); !ok     { return true } else {return d.val.After(other.(date).val) } }
func (d duration) Greater(other Value) bool{ if _,ok := other.(duration); !ok { return true } else {return d.val > other.(duration).val } }
func (t text) Greater(other Value) bool    { if _,ok := other.(text); !ok     { return true } else {return t > other.(text) } }
func (n null) Greater(other Value) bool    { if o,ok := other.(null); ok      { return n > o } else {return false} }
func (l liker) Greater(other Value) bool   { return false }

func (f float) GreatEq(other Value) bool   { if _,ok:=other.(float);!ok    {return true};return f >= other.(float) }
func (i integer) GreatEq(other Value) bool { if _,ok:=other.(integer);!ok  {return true};return i >= other.(integer) }
func (d date) GreatEq(other Value) bool    { if _,ok:=other.(date);!ok     {return true};return !d.val.Before(other.(date).val) }
func (d duration) GreatEq(other Value) bool{ if _,ok:=other.(duration);!ok {return true};return d.val > other.(duration).val }
func (t text) GreatEq(other Value) bool    { if _,ok:=other.(text);!ok     {return true};return t >= other.(text) }
func (n null) GreatEq(other Value) bool    { return false }
func (l liker) GreatEq(other Value) bool   { return false }

func (f float) Equal(other Value) bool     { if _,ok:=other.(float);!ok     {return false};return f == other.(float) }
func (i integer) Equal(other Value) bool   { if _,ok:=other.(integer);!ok   {return false};return i == other.(integer) }
func (d date) Equal(other Value) bool      { if _,ok:=other.(date);!ok      {return false};return d.val.Equal(other.(date).val) }
func (d duration) Equal(other Value) bool  { if _,ok:=other.(duration);!ok  {return false};return d.val == other.(duration).val }
func (t text) Equal(other Value) bool      { if _,ok:=other.(text);!ok      {return false};return t == other.(text) }
func (n null) Equal(other Value) bool      { if _,ok := other.(null);ok     {return true };return false }
func (l liker) Equal(other Value) bool     { return l.val.MatchString(Sprint(other)) }

func (d duration) Add(other Value) Value {
	switch o := other.(type) {
		case date:     return date{o.val.Add(d.val)}
		case duration: return duration{d.val + o.val}
		case null:     return o
	}
	return d
}
func (d duration) Sub(other Value) Value {
	switch o := other.(type) {
		case date:     return date{o.val.Add(- d.val)}
		case duration: return duration{d.val - o.val}
		case null:     return o
	}
	return d
}

func (f float) Add(other Value) Value   { if _,ok:=other.(float);!ok      {return other};return float(f + other.(float)) }
func (i integer) Add(other Value) Value { if _,ok:=other.(integer);!ok    {return other};return integer(i + other.(integer)) }
func (d date) Add(other Value) Value    { if _,ok:=other.(duration);!ok   {return other};return date{d.val.Add(other.(duration).val)} }
func (t text) Add(other Value) Value    { if _,ok:=other.(text);!ok       {return other};return text(t + other.(text)) }
func (n null) Add(other Value) Value    { return n }
func (l liker) Add(other Value) Value   { return l }

func (f float) Sub(other Value) Value   { if _,ok:=other.(float);!ok   {return other};return float(f - other.(float)) }
func (i integer) Sub(other Value) Value { if _,ok:=other.(integer);!ok {return other};return integer(i - other.(integer)) }
func (d date) Sub(other Value) Value    {
	switch o := other.(type) {
		case date:     return duration{d.val.Sub(o.val)}
		case duration: return date{d.val.Add(-o.val)}
		case null:     return o
	}
	return d
}
func (t text) Sub(other Value) Value    { return t }
func (n null) Sub(other Value) Value    { return n }
func (l liker) Sub(other Value) Value   { return l }

func (f float) Mult(other Value) Value  {
	switch o := other.(type) {
		case float:    return float(f * o)
		case integer:  return float(f * float(o))
		case duration: return duration{time.Duration(f) * o.val}
		case null:     return o
	}
	return f
}
func (i integer) Mult(other Value) Value{
	switch o := other.(type) {
		case integer:  return integer(i * o)
		case duration: return duration{time.Duration(i) * o.val}
		case null:     return o
	}
	return i
}
func (d date) Mult(other Value) Value     { return d }
func (d duration) Mult(other Value) Value {
	switch o := other.(type) {
		case integer: return duration{d.val * time.Duration(o)}
		case float:   return duration{d.val * time.Duration(o)}
		case null:    return o
	}
	return d
}
func (t text) Mult(other Value) Value     { return t }
func (n null) Mult(other Value) Value     { return n }
func (l liker) Mult(other Value) Value    { return l }

func (f float) Div(other Value) Value    {
	switch o := other.(type) {
		case float:   if o != 0 { return float(f / o)        } else { return null("") }
		case integer: if o != 0 { return float(f / float(o)) } else { return null("") }
		case null:     return o
	}
	return f
}
func (i integer) Div(other Value) Value  {
	switch o := other.(type) {
		case integer: if o != 0 { return integer(i / o)          } else { return null("") }
		case float:   if o != 0 { return integer(i / integer(o)) } else { return null("") }
		case null:     return o
	}
	return i
}
func (d date) Div(other Value) Value     { return d }
func (d duration) Div(other Value) Value {
	switch o := other.(type) {
		case integer: if o != 0 { return duration{d.val / time.Duration(o)} } else { return null("") }
		case float:   if o != 0 { return duration{d.val / time.Duration(o)} } else { return null("") }
		case null:     return o
	}
	return d
}
func (t text) Div(other Value) Value     { return t }
func (n null) Div(other Value) Value     { return n }
func (l liker) Div(other Value) Value    { return l }

func (f float) Mod(other Value) Value    { return f }
func (i integer) Mod(other Value) Value  { return integer(i % other.(integer)) }
func (d date) Mod(other Value) Value     { return d }
func (d duration) Mod(other Value) Value { return d }
func (t text) Mod(other Value) Value     { return t }
func (n null) Mod(other Value) Value     { return n }
func (l liker) Mod(other Value) Value    { return l }

func (f float) String() string    { return Sprintf("%.10g",f) }
func (i integer) String() string  { return Sprintf("%d",i) }
func (d date) String() string     { return d.val.Format("2006-01-02 15:04:05") }
func (d duration) String() string { return d.val.String() }
func (t text) String() string     { return string(t) }
func (n null) String() string     { return string(n) }
func (l liker) String() string    { return Sprint(l.val) }

func (f float) MarshalJSON() ([]byte,error)    { return json.Marshal(f.String()) }
func (i integer) MarshalJSON() ([]byte,error)  { return json.Marshal(i.String())}
func (d date) MarshalJSON() ([]byte,error)     { return json.Marshal(d.String()) }
func (d duration) MarshalJSON() ([]byte,error) { return json.Marshal(d.String()) }
func (t text) MarshalJSON() ([]byte,error)     { return json.Marshal(t.String()) }
func (n null) MarshalJSON() ([]byte,error)     { return json.Marshal(n.String()) }
func (l liker) MarshalJSON() ([]byte,error)    { return json.Marshal(l.String())}
