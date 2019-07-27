package main
import (
  . "fmt"
  "encoding/csv"
  "os"
  . "strconv"
  "sort"
  "io"
  "bytes"
)

var stop int
var active bool

//Random access csv reader
type LineReader struct {
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
	val Value
}
func (l*LineReader) SavePos(value Value) {
	l.valPositions = append(l.valPositions, ValPos{l.prevPos, value})
}
func (l*LineReader) PrepareReRead() {
	l.lineBytes = make([]byte, l.maxLineSize)
	l.byteReader = bytes.NewReader(l.lineBytes)
}
func (l*LineReader) Init(q *QuerySpecs, f string) {
	l.fp,_ = os.Open(q.files[f].fname)
	l.valPositions = make([]ValPos,0)
	l.tee = io.TeeReader(l.fp, &l.lineBuffer)
	l.csvReader = csv.NewReader(l.tee)
	if q.quantityLimit == 0 { l.limit = 1<<62 } else { l.limit = q.quantityLimit }
	l.Read()
}

func (l*LineReader) Read() ([]string,error) {
	line, err := l.csvReader.Read()
	l.lineBytes, _ = l.lineBuffer.ReadBytes('\n')
	size := len(l.lineBytes)
	if l.maxLineSize < size { l.maxLineSize = size }
	l.prevPos = l.pos
	l.pos += int64(size)
	return line, err
}
func (l*LineReader) ReadAt(lineNo int) ([]string,error) {
	l.fp.ReadAt(l.lineBytes, l.valPositions[lineNo].pos)
	l.byteReader.Seek(0,0)
	l.csvReader = csv.NewReader(l.byteReader)
	line, err := l.csvReader.Read()
	return line, err
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
		Types: q.colSpec.NewTypes,
		Pos: q.colSpec.NewPos,
		ShowLimit : q.showLimit,
	}

	//prepare reader and run query
	var reader LineReader
	reader.Init(q, "_f1")
	defer func(){ active=false; if q.save {saver <- saveData{Type:CH_NEXT}}; reader.fp.Close() }()
	if q.sortExpr != nil && !q.groupby {
		err = orderedQuery(q, &res, &reader)
	} else {
		err = normalQuery(q, &res, &reader)
	}
	if err != nil { Println(err); return SingleQueryResult{}, err }
	res.Numrows = q.quantityRetrieved
	returnGroupedRows(q, &res)
	res.Numcols = q.colSpec.NewWidth
	return res, nil
}

//retrieve results without needing to index the rows
func normalQuery(q *QuerySpecs, res *SingleQueryResult, reader *LineReader) error {
	var err error
	rowsChecked := 0
	stop = 0
	distinctCheck := make(map[interface{}]bool)

	for {
		if stop == 1 { stop = 0;  break }
		if q.LimitReached() && !q.groupby { break }

		//read line from csv file
		q.fromRow,err = reader.Read()
		if err != nil {break}

		//find matches and retrieve results
		if evalWhere(q) && evalDistinct(q, distinctCheck) && execGroupOrNewRow(q,q.tree.node4) {
			execSelect(q, res)
		}

		//periodic updates
		rowsChecked++
		if rowsChecked % 10000 == 0 { message("Scanning line "+Itoa(rowsChecked)+", "+Itoa(q.quantityRetrieved)+" results so far") }
	}
	return nil
}

//see if row has distinct value if looking for one
func evalDistinct(q *QuerySpecs, distinctCheck map[interface{}]bool) bool {
	if q.distinctExpr == nil { return true }
	_,compVal := execExpression(q, q.distinctExpr)
	_,duplicate := distinctCheck[compVal]
	if duplicate {
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
		if rowsChecked % 10000 == 0 { message("Scanning line "+Itoa(rowsChecked)) }
		q.fromRow,err = reader.Read()
		if err != nil {break}
		match = evalWhere(q)
		if match {
			_,sortExpr := execExpression(q, q.sortExpr)
			reader.SavePos(sortExpr)
		}
	}

	//sort matching line positions
	message("Sorting Rows...")
	sort.Slice(reader.valPositions, func(i, j int) bool {
		ret := reader.valPositions[i].val.Greater(reader.valPositions[j].val)
		if q.sortWay == 2 { return !ret }
		return ret
	})

	//go back and retrieve lines in the right order
	reader.PrepareReRead()
	for i := range reader.valPositions {
		if stop == 1 { stop = 0; message("query cancelled"); break }
		q.fromRow,err = reader.ReadAt(i)
		if err != nil { break }
		if evalDistinct(q, distinctCheck) {
			execGroupOrNewRow(q,q.tree.node4)
			execSelect(q, res)
			if q.LimitReached() { break }
			if q.quantityRetrieved % 1000 == 0 { message("Retrieving line "+Itoa(q.quantityRetrieved)) }
		}
	}
	return nil
}

func groupRetriever (q *QuerySpecs, n* Node, m map[interface{}]interface{}, r *SingleQueryResult){
	switch n.tok1.(int) {
	case 0:
		for k,row := range m {
			q.midRow = row.([]Value)
			if evalHaving(q) {
				q.toRow = make([]Value, q.colSpec.NewWidth)
				execSelect(q,r)
				r.Vals = append(r.Vals, q.toRow[0:q.colSpec.NewWidth-q.midExess])
				m[k] = nil
				q.quantityRetrieved++
				if q.LimitReached() && !q.save && q.sortExpr==nil { return }
			}
		}
	case 1: for _,v := range m { groupRetriever(q, n.node2, v.(map[interface{}]interface{}), r) }
	}
}
func returnGroupedRows(q *QuerySpecs, res *SingleQueryResult) {
	if !q.groupby { return }
	root := q.tree.node4
	q.stage = 1
	q.quantityRetrieved = 0
	//make map for single group so it gets processed with that system
	if root == nil {
		map1 := make(map[interface{}]interface{})
		map1[0] = q.toRow
		root = &Node{ tok1: map1, node1: &Node{ tok1: 0}}
	}
	groupRetriever(q, root.node1, root.tok1.(map[interface{}]interface{}), res)
	//sort groups
	if q.sortExpr != nil {
		message("Sorting Rows...")
		sortIndex := len(res.Vals[0])-1
		sort.Slice(res.Vals, func(i, j int) bool {
			ret := res.Vals[i][sortIndex].Greater(res.Vals[j][sortIndex])
			if q.sortWay == 2 { return !ret }
			return ret
		})
		//remove sort value and excess rows when done
		if q.quantityLimit > 0 && q.quantityLimit <= len(res.Vals) { res.Vals = res.Vals[0:q.quantityLimit] }
		for i,_ := range res.Vals { res.Vals[i] = res.Vals[i][0:sortIndex] }
		q.colSpec.NewWidth--
	}
	//save groups to file
	if q.save  {
		for _,v := range res.Vals { saver <- saveData{Type : CH_ROW, Row : &v} ; <-savedLine }
	}
}
