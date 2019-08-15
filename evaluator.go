package main
import (
	. "fmt"
	"encoding/csv"
	"os"
	. "strconv"
	"sort"
	"io"
	"bytes"
	bt "github.com/google/btree"
)

var stop int
var active bool

//Random access csv reader
type LineReader struct {
	valPositions []ValPos
	lineBytes []byte
	fromRow []string
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
func (l*LineReader) SavePosTo(value Value, arr *[]ValPos) {
	*arr = append(*arr, ValPos{l.prevPos, value})
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
	var err error
	l.fromRow, err = l.csvReader.Read()
	l.lineBytes, _ = l.lineBuffer.ReadBytes('\n')
	size := len(l.lineBytes)
	if l.maxLineSize < size { l.maxLineSize = size }
	l.prevPos = l.pos
	l.pos += int64(size)
	return l.fromRow, err
}
func (l*LineReader) ReadAtIndex(lineNo int) ([]string,error) {
	return l.ReadAtPosition(l.valPositions[lineNo].pos)
}
func (l*LineReader) ReadAtPosition(pos int64) ([]string,error) {
	l.fp.ReadAt(l.lineBytes, pos)
	l.byteReader.Seek(0,0)
	l.csvReader = csv.NewReader(l.byteReader)
	var err error
	l.fromRow, err = l.csvReader.Read()
	return l.fromRow, err
}

//run csv query
func CsvQuery(q *QuerySpecs) (SingleQueryResult, error) {

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

	defer func(){
		active=false
		if q.save {saver <- saveData{Type:CH_NEXT}}
		for ii := 1; ii <= q.numfiles; ii++ {
			q.files["_f"+Sprint(ii)].reader.fp.Close()
		}
	}()

	if q.sortExpr != nil && !q.groupby {
		err = orderedQuery(q, &res)
	} else if q.joining {
		err = joinQuery(q, &res)
	} else {
		err = normalQuery(q, &res)
	}
	if err != nil { Println(err); return SingleQueryResult{}, err }
	res.Numrows = q.quantityRetrieved
	returnGroupedRows(q, &res)
	res.Numcols = q.colSpec.NewWidth
	return res, nil
}

//retrieve results without needing to index the rows
func normalQuery(q *QuerySpecs, res *SingleQueryResult) error {
	var err error
	rowsChecked := 0
	stop = 0
	distinctCheck := bt.New(200)
	reader := q.files["_f1"].reader

	for {
		if stop == 1 { stop = 0;  break }
		if q.LimitReached() && !q.groupby { break }

		//read line from csv file
		_,err = reader.Read()
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
func evalDistinct(q *QuerySpecs, distinctCheck *bt.BTree) bool {
	if q.distinctExpr == nil { return true }
	_,compVal := execExpression(q, q.distinctExpr)
	return distinctCheck.ReplaceOrInsert(compVal) == nil
}

//run ordered query
func orderedQuery(q *QuerySpecs, res *SingleQueryResult) error {
	stop = 0
	distinctCheck := bt.New(200)
	reader := q.files["_f1"].reader
	rowsChecked := 0
	var match bool
	var err error
	//initial scan to find line positions
	for {
		if stop == 1 { break }
		rowsChecked++
		if rowsChecked % 10000 == 0 { message("Scanning line "+Itoa(rowsChecked)) }
		_,err = reader.Read()
		if err != nil {break}
		match = evalWhere(q)
		if match {
			_,sortExpr := execExpression(q, q.sortExpr)
			reader.SavePos(sortExpr)
		}
	}

	//sort matching line positions
	message("Sorting Rows...")
	if !flags.gui() { print("\n") }
	sort.Slice(reader.valPositions, func(i, j int) bool {
		ret := reader.valPositions[i].val.Greater(reader.valPositions[j].val)
		if q.sortWay == 2 { return !ret }
		return ret
	})

	//go back and retrieve lines in the right order
	reader.PrepareReRead()
	for i := range reader.valPositions {
		if stop == 1 { stop = 0; message("query cancelled"); break }
		_,err = reader.ReadAtIndex(i)
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
		if !flags.gui() { print("\n") }
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

//join query
func joinQuery(q *QuerySpecs, res *SingleQueryResult) error {
	var err error
	stop = 0
	reader1 := q.files["_f1"].reader
	scanJoinFiles(q,q.tree.node2)
	firstJoin := q.tree.node2.node1
	distinctCheck := bt.New(200)
	for {
		if stop == 1 { stop = 0;  break }
		_,err = reader1.Read()
		if err != nil {break}
		for nn := firstJoin ; nn != nil ; nn = nn.node2 { //process each join
			predNode := nn.node1.node1
			jf := predNode.tok5.(JoinFinder)
			jreader := q.files[jf.jfile].reader
			_,compVal := execExpression(q, jf.baseNode)
			joinCount := 0
			for pos := jf.FindNext(compVal); pos != -1 ; pos = jf.FindNext(compVal) { //process each match
				jreader.ReadAtPosition(pos)
				joinCount++
				if q.LimitReached() { goto done1 }
				if evalWhere(q) && evalDistinct(q, distinctCheck) && execGroupOrNewRow(q,q.tree.node4) {
					execSelect(q, res)
				}
			}
			if joinCount == 0 && nn.tok1.(int) == 1 { //left join when no match
				if q.LimitReached() { goto done1 }
				for k,_ := range jreader.fromRow { jreader.fromRow[k] = "" }
				if evalWhere(q) && evalDistinct(q, distinctCheck) && execGroupOrNewRow(q,q.tree.node4) {
					execSelect(q, res)
				}
			}
		}
		done1:
	}
	return nil
}
//scan and sort values from join files
func scanJoinFiles(q *QuerySpecs, n *Node) {
	if n == nil { return }
	var err error
	if n.label == N_PREDCOMP {
		reader := q.files[n.tok5.(JoinFinder).jfile].reader
		jf := n.tok5.(JoinFinder)
		onExpr := jf.joinNode
		for {
			_,err = reader.Read()
			if err != nil {break}
			_,onValue := execExpression(q, onExpr)
			reader.SavePosTo(onValue, &jf.arr)
		}
		jf.Sort()
		reader.PrepareReRead()
		n.tok5 = jf
	} else {
		scanJoinFiles(q, n.node1)
		scanJoinFiles(q, n.node2)
	}
	return
}
