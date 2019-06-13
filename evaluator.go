package main
import (
  . "fmt"
  "encoding/csv"
  "os"
  s "strings"
  d "github.com/araddon/dateparse"
  . "strconv"
  "time"
  "sort"
  "io"
  "bytes"
)

var stop int
var active bool

//Random access csv reader
type LineReader struct {
	results []interface{}
	types []int
	valPositions []ValPos
	lineBytes []byte
	limit int
	maxLineSize int
	pos int64
	prevPos int64
	lineBuffer bytes.Buffer
	tee io.Reader
	csvReader *csv.Reader
	byteReader *bytes.Reader
	fp *os.File
}
type ValPos struct {
	pos int64
	val interface{}
}
func (l*LineReader) SavePos(colNo int) {
	l.valPositions = append(l.valPositions, ValPos{l.prevPos, l.results[colNo]})
}
func (l*LineReader) PrepareReRead() {
	l.lineBytes = make([]byte, l.maxLineSize)
	l.byteReader = bytes.NewReader(l.lineBytes)
}
func (l*LineReader) Init(q *QuerySpecs, f string) {
	l.types = q.files[f].types
	l.fp,_ = os.Open(q.files[f].fname)
	l.valPositions = make([]ValPos,0)
	l.tee = io.TeeReader(l.fp, &l.lineBuffer)
	l.csvReader = csv.NewReader(l.tee)
	l.results = make([]interface{}, q.files[f].width)
	if q.quantityLimit == 0 { l.limit = 1<<62 } else { l.limit = q.quantityLimit }
	l.Read()
}
func (l*LineReader) convertLine(inline *[]string) {
	for i,cell := range (*inline) {
		cell = s.TrimSpace(cell)
		if s.ToLower(cell) == "null" || cell == "" { l.results[i] = nil
		} else {
			switch l.types[i] {
				case T_INT:	l.results[i],_ = Atoi(cell)
				case T_FLOAT:  l.results[i],_ = ParseFloat(cell,64)
				case T_DATE:   l.results[i],_ = d.ParseAny(cell)
				case T_NULL:   fallthrough
				case T_STRING: l.results[i] = cell
			}
		}
	}
}
func (l*LineReader) Read() ([]interface{},error) {
	line, err := l.csvReader.Read()
	l.lineBytes, _ = l.lineBuffer.ReadBytes('\n')
	size := len(l.lineBytes)
	if l.maxLineSize < size { l.maxLineSize = size }
	l.prevPos = l.pos
	l.pos += int64(size)
	l.convertLine(&line)
	return l.results, err
}
func (l*LineReader) ReadAt(lineNo int) ([]interface{},error) {
	l.fp.ReadAt(l.lineBytes, l.valPositions[lineNo].pos)
	l.byteReader.Seek(0,0)
	l.csvReader = csv.NewReader(l.byteReader)
	line, err := l.csvReader.Read()
	l.convertLine(&line)
	return l.results, err
}

//run csv query
func csvQuery(q *QuerySpecs) (SingleQueryResult, error) {

	//parse and do stuff that only needs to be done once
	var err error
	q.tree,err = parseQuery(q)
	if err != nil { Println(err); return SingleQueryResult{}, err }
	if q.save { saver <- saveData{Type : CH_HEADER, Header : q.colSpec.NewNames}; <-savedLine }
	q.showLimit = 20000 / len(q.colSpec.NewNames)
	active = true

	//prepare output
	res:= SingleQueryResult{
		Colnames : q.colSpec.NewNames,
		Numcols: q.colSpec.NewWidth,
		Types: q.colSpec.NewTypes,
		Pos: q.colSpec.NewPos,
		ShowLimit : q.showLimit,
	}

	//prepare reader and run query
	var reader LineReader
	reader.Init(q, "_fmk01")
	defer func(){ active=false; if q.save {saver <- saveData{Type:CH_NEXT}}; reader.fp.Close() }()
	if q.sortWay == 0 {
		err = normalQuery(q, &res, &reader)
	} else {
		err = orderedQuery(q, &res, &reader)
	}
	if err != nil { Println(err); return SingleQueryResult{}, err }
	return res, nil
}

//retrieve results on first pass
func normalQuery(q *QuerySpecs, res *SingleQueryResult, reader *LineReader) error {
	var err error
	rowsChecked := 0
	stop = 0
	distinctCheck := make(map[interface{}]bool)
	for ;res.Numrows<reader.limit; {
		if stop == 1 { stop = 0; messager <- "query cancelled"; break }

		//read line from csv file
		q.fromRow,err = reader.Read()
		if err != nil {break}

		//find matches and retrieve results
		//modified to use new fromrow, new version won't need that param
		match := eval2Where(q)
		//match,err := evalWhere(q, &q.fromRow)
		//if err != nil {return err}
		if match && evalDistinct(q, distinctCheck) {
			exec2Select(q, res)
			//execSelect(q, res, &fromRow)
			res.Numrows++;
		}

		//periodic updates
		rowsChecked++
		if rowsChecked % 10000 == 0 { messager <- "Scanning line "+Itoa(rowsChecked)+", "+Itoa(res.Numrows)+" matches so far" }
	}
	return nil
}

//see if row has distinct value if looking for one
func evalDistinct(q *QuerySpecs, distinctCheck map[interface{}]bool) bool {
	if q.distinctExpr == nil { return true }
	_,compVal := execExpression(q, q.distinctExpr)
	//ok means not distinct
	_,ok := distinctCheck[compVal]
	if ok {
		return false
	} else {
		distinctCheck[compVal] = true
	}
	return true
}

//run ordered query
func orderedQuery(q *QuerySpecs, res *SingleQueryResult, reader *LineReader) error {
	stop = 0
	distinctCheck := make(map[interface{}]bool)
	rowsChecked := 0
	var match bool
	var err error
	//initial scan to find line positions
	for {
		if stop == 1 { break }
		rowsChecked++
		if rowsChecked % 10000 == 0 { messager <- "Scanning line "+Itoa(rowsChecked) }
		q.fromRow,err = reader.Read()
		if err != nil {break}
		match = eval2Where(q)
		if match { reader.SavePos(q.sortCol) }
	}

	//sort matching line positions
	messager <- "Sorting Rows..."
	colType := reader.types[q.sortCol]
	sort.Slice(reader.valPositions, func(i, j int) bool {
		if reader.valPositions[i].val == nil && reader.valPositions[j].val == nil { return false
		} else if reader.valPositions[i].val == nil { return false
		} else if reader.valPositions[j].val == nil { return true }
		ret := false
		switch colType {
			case T_NULL:   fallthrough
			case T_STRING: ret = reader.valPositions[i].val.(string)        > reader.valPositions[j].val.(string)
			case T_INT:	ret = reader.valPositions[i].val.(int)              > reader.valPositions[j].val.(int)
			case T_FLOAT:  ret = reader.valPositions[i].val.(float64)       > reader.valPositions[j].val.(float64)
			case T_DATE:   ret = reader.valPositions[i].val.(time.Time).After(reader.valPositions[j].val.(time.Time))
		}
		if q.sortWay == 2 { return !ret }
		return ret
	})

	//go back and retrieve lines in the right order
	reader.PrepareReRead()
	for i := range reader.valPositions {
		if stop == 1 { stop = 0; messager <- "query cancelled"; break }
		q.fromRow,err = reader.ReadAt(i)
		if err != nil { break }
		if evalDistinct(q, distinctCheck) {
			exec2Select(q, res)
			res.Numrows++;
			if res.Numrows >= reader.limit { break }
			if res.Numrows % 1000 == 0 { messager <- "Retrieving line "+Itoa(res.Numrows) }
		}
	}
	return nil
}
