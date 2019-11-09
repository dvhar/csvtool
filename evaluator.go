package main
import (
	. "fmt"
	"sort"
	bt "github.com/google/btree"
)

var stop int
var active bool

//run csv query
func CsvQuery(q *QuerySpecs) (SingleQueryResult, error) {

	//parse and do stuff that only needs to be done once
	var err error
	q.tree,err = parseQuery(q)
	if err != nil { Println(err); return SingleQueryResult{}, err }
	if q.save { saver <- saveData{Type : CH_HEADER, Header : q.colSpec.NewNames}; <-savedLine }
	q.showLimit = 20000 / len(q.colSpec.NewNames)
	q.distinctCheck = bt.New(200)
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

	if q.sortExpr != nil && !q.groupby && q.joining {
		q.gettingSortVals = true
		err = orderedJoinQuery(q, &res)
	} else if q.sortExpr != nil && !q.groupby {
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
	reader := q.files["_f1"].reader
	notifier := TimedNotifier("Scanning line ",&rowsChecked,", ",&q.quantityRetrieved," results so far")
	notifier(START)

	for {
		if stop == 1 { stop = 0;  break }
		if q.LimitReached() && !q.groupby { break }

		//read line from csv file
		_,err = reader.Read()
		if err != nil {break}

		//find matches and retrieve results
		if evalWhere(q) && evalDistinct(q) && execGroupOrNewRow(q,q.tree.node4) {
			execSelect(q, res)
		}

		rowsChecked++
	}
	notifier(STOP)
	return nil
}

//see if row has distinct value if looking for one
func evalDistinct(q *QuerySpecs) bool {
	if q.distinctExpr == nil { return true }
	_,compVal := execExpression(q, q.distinctExpr)
	return q.distinctCheck.ReplaceOrInsert(compVal) == nil
}

//run ordered query
func orderedQuery(q *QuerySpecs, res *SingleQueryResult) error {
	stop = 0
	reader := q.files["_f1"].reader
	rowsChecked := 0
	var match bool
	var err error
	notifier := TimedNotifier("Scanning line ",&rowsChecked)
	notifier(START)

	//initial scan to find line positions
	for {
		if stop == 1 { break }
		rowsChecked++
		_,err = reader.Read()
		if err != nil {break}
		match = evalWhere(q)
		if match {
			_,sortExpr := execExpression(q, q.sortExpr)
			reader.SavePos(sortExpr)
		}
	}
	notifier(STOP)

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
	notifier = TimedNotifier("Retrieving line ",&q.quantityRetrieved)
	notifier(START)
	for i := range reader.valPositions {
		if stop == 1 { stop = 0; message("query cancelled"); break }
		_,err = reader.ReadAtIndex(i)
		if err != nil { break }
		if evalDistinct(q) {
			execGroupOrNewRow(q,q.tree.node4)
			execSelect(q, res)
			if q.LimitReached() { break }
		}
	}
	notifier(STOP)
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

//ordered non-grouping join query
func orderedJoinQuery(q *QuerySpecs, res *SingleQueryResult) error {
	var err error
	stop = 0
	reader1 := q.files["_f1"].reader
	scanJoinFiles(q,q.tree.node2,false)
	firstJoin := q.tree.node2.node1
	notifier := TimedNotifier("Finding join ",&q.quantityRetrieved)
	notifier(START)
	for {
		if stop == 1 { stop = 0; return nil }
		_,err = reader1.Read()
		if err != nil {break}
		if joinNextFile(q, res, firstJoin) { break }
	}
	notifier(STOP)
	reader2 := q.files["_f2"].reader
	message("Sorting Rows...")
	if !flags.gui() { print("\n") }
	sort.Slice(q.joinSortVals, func(i, j int) bool {
		ret := q.joinSortVals[i].val.Greater(q.joinSortVals[j].val)
		if q.sortWay == 2 { return !ret }
		return ret
	})
	q.quantityRetrieved = 0
	reader1.PrepareReRead()
	reader2.PrepareReRead()
	joinRows := firstJoin.node1.node1.tok5.(JoinFinder).rowArr
	message("Joining files...")
	notifier = TimedNotifier("Retrieving joined line ",&q.quantityRetrieved)
	notifier(START)
	for _,v := range q.joinSortVals {
		if stop == 1 { stop = 0;  break }
		reader1.ReadAtPosition(v.pos1)
		if q.bigjoin {
			reader2.ReadAtPosition(v.pos2)
		} else {
			reader2.fromRow = joinRows[v.pos2].row
		}
		q.toRow = make([]Value, q.colSpec.NewWidth)
		execSelect(q, res)
		q.quantityRetrieved++
		if q.LimitReached() { return nil }
	}
	notifier(STOP)
	return nil
}

//join query
func joinQuery(q *QuerySpecs, res *SingleQueryResult) error {
	var err error
	stop = 0
	reader1 := q.files["_f1"].reader
	scanJoinFiles(q,q.tree.node2,false)
	firstJoin := q.tree.node2.node1
	notifier := TimedNotifier("Retrieving joined line ",&q.quantityRetrieved)
	notifier(START)
	for {
		if stop == 1 { stop = 0;  break }
		_,err = reader1.Read()
		if err != nil {break}
		if joinNextFile(q, res, firstJoin) { break }
	}
	notifier(STOP)
	return nil
}
//returns 'reached limit' bool
func joinNextFile(q *QuerySpecs, res *SingleQueryResult, nn *Node) bool {
	if nn == nil { //have a line from each file so time to query
		if evalWhere(q) && evalDistinct(q) && execGroupOrNewRow(q,q.tree.node4) { //TODO:stop redunant newrow on join sort
			if q.gettingSortVals {
				_,sortExpr := execExpression(q, q.sortExpr)
				q.SaveJoinPos(sortExpr)
			} else {
				execSelect(q, res)
				if q.LimitReached() { return true }
			}
		}
		return false
	}
	predNode := nn.node1.node1
	jf := predNode.tok5.(JoinFinder)
	jreader := q.files[jf.jfile].reader
	_,compVal := execExpression(q, jf.baseNode)
	joinFound := false
	//process each match for big file
	if nn.tok2.(int) == 1 {
		for pos := jf.FindNextBig(compVal); pos != -1 ; pos = jf.FindNextBig(compVal) {
			joinFound = true
			jreader.ReadAtPosition(pos)
			if joinNextFile(q,res,nn.node2) { return true }
		}
	//process each match for small file
	} else {
		for vrow := jf.FindNextSmall(compVal); vrow != nil ; vrow = jf.FindNextSmall(compVal) {
			joinFound = true
			jreader.fromRow = vrow.row
			jreader.index = int64(vrow.idx)
			if joinNextFile(q,res,nn.node2) { return true }
		}
	}
	//left join when no match
	if !joinFound && nn.tok1.(int) == 1 {
		if jreader.fromRow == nil { jreader.fromRow = make([]string, q.files[jf.jfile].width) }
		for k,_ := range jreader.fromRow { jreader.fromRow[k] = "" }
		if joinNextFile(q,res,nn.node2) { return true }
	}
	return false
}

//scan and sort values from join files for binary search
func scanJoinFiles(q *QuerySpecs, n *Node, big bool) {
	if n == nil { return }
	var err error
	if n.label == N_JOIN {
		if n.tok2.(int) == 1 {
			big = true
		} else {
			big = false
		}
	}
	if n.label == N_PREDCOMP {
		reader := q.files[n.tok5.(JoinFinder).jfile].reader
		jf := n.tok5.(JoinFinder)
		i := 1
		if big {
			notifier := TimedNotifier("Indexing join value ",&i)
			notifier(START)
			for {
				i++
				_,err = reader.Read()
				if err != nil {break}
				if stop == 1 { break }
				_,onValue := execExpression(q, jf.joinNode)
				if _,ok := onValue.(null); !ok {
					reader.SavePosTo(onValue, &jf.posArr)
				}
			}
			notifier(STOP)
		} else {
			rowIdx := 0
			for {
				_,err = reader.Read()
				if err != nil {break}
				_,onValue := execExpression(q, jf.joinNode)
				if _,ok := onValue.(null); !ok {
					newRow := make([]string, len(reader.fromRow))
					copy(newRow, reader.fromRow)
					jf.rowArr = append(jf.rowArr, ValRow{newRow, onValue, rowIdx})
					rowIdx++
				}
			}
		}
		message("Sorting indeces...")
		jf.Sort()
		reader.PrepareReRead()
		n.tok5 = jf
	} else {
		scanJoinFiles(q, n.node1, big)
		scanJoinFiles(q, n.node2, big)
	}
	return
}
