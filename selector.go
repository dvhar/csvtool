package main
import (
	. "strconv"
	d "github.com/araddon/dateparse"
	s "strings"
	//. "fmt"
	//r "reflect"
)

func execSelect(q *QuerySpecs, res*SingleQueryResult) {

	execSelections(q, q.tree.node1.node1)

	//add non-grouped row to web return data
	if q.quantityRetrieved <= q.showLimit && !q.groupby { res.Vals = append(res.Vals, q.toRow) }

	//save non-grouped row
	if q.save && !q.groupby { saver <- saveData{Type : CH_ROW, Row : &q.toRow} ; <-savedLine}
}

func execSelections(q *QuerySpecs, n *Node) {
	if n == nil { return }
	index := n.tok1.([]int)[q.stage]
	var val Value

	//reading from csv file
	if q.stage == 0 {
		_,val = execExpression(q, n.node1)
		//evaluated whole expression on first pass
		if n.tok4 == nil { q.toRow[index] = val }

	//reading from aggregate groups
	} else {
		//expression was already evaluated
		if n.tok4 == nil {
			q.toRow[index] = q.midRow[n.tok1.([]int)[0]]
		//finish aggregate expression
		} else {
			_,val = execExpression(q, n.node1)
			q.toRow[index] = val
		}
	}

	execSelections(q, n.node2)
}

func evalWhere(q *QuerySpecs) bool {
	node := q.tree.node3
	if node.node1 == nil { return true }
	return evalPredicates(q, node.node1)
}

func evalHaving(q *QuerySpecs) bool {
	node := q.tree.node5
	if node.node1 == nil { return true }
	return evalPredicates(q, node.node1)
}

//set target row and return bool for valid row
func execGroupOrNewRow(q *QuerySpecs, n *Node) bool {
	//not grouping
	if !q.groupby { q.toRow = make([]Value, q.colSpec.NewWidth); q.quantityRetrieved++; return true }

	//grouping to a single row because no groupby clause
	if n == nil {
		if q.toRow == nil { q.toRow = make([]Value, q.colSpec.AggregateCount); q.quantityRetrieved++ }
		return true
	}
	//grouping with groupby clause
	return execGroupExpressions(q, n.node1, n.tok1.(map[interface{}]interface{}))
}
func execGroupExpressions(q *QuerySpecs, n *Node, m map[interface{}]interface{}) bool {
	_, key := execExpression(q,n.node1)
	switch n.tok1.(int) {
	case 0:
		row,ok := m[key]
		if ok {
			q.toRow = row.([]Value)
			return true
		} else if !q.LimitReached() || q.sortExpr != nil {
			q.quantityRetrieved++
			row = make([]Value, q.colSpec.AggregateCount)
			m[key] = row
			q.toRow = row.([]Value)
			return true
		} else { return false }
	case 1:
		nextMap,ok := m[key]
		if ok {
			return execGroupExpressions(q, n.node2, nextMap.(map[interface{}]interface{}))
		} else {
			nextMap = make(map[interface{}]interface{})
			m[key] = nextMap
			return execGroupExpressions(q, n.node2, nextMap.(map[interface{}]interface{}))
		}
	}
	return false
}

//returns type and value
func execExpression(q *QuerySpecs, n *Node) (int,Value) {
	switch n.label {

	case N_FUNCTION:
		//return earlier aggregated value if in group retrieval stage
		if q.stage == 1 {
			agg := q.midRow[n.tok2.(int)]
			if avg,ok := agg.(AverageVal);ok { return 1, avg.Eval() }
			return 1, agg
		}

		//values have not been aggregated yet
		t1,v1 := execExpression(q, n.node1)
		if _,ok:=v1.(null);!ok {
			//non-aggregate function
			if n.tok2==nil {
				switch n.tok1.(int) {
				case FN_ABS:        if v1.Less(integer(0)) { v1 = v1.Mult(integer(-1)) }
				case FN_YEAR:       v1 = integer(v1.(date).val.Year())
				case FN_MONTH:      v1 = integer(v1.(date).val.Month())
				case FN_WEEK:       v1 = integer(v1.(date).val.YearDay() / 7)
				case FN_YDAY:       v1 = integer(v1.(date).val.YearDay())
				case FN_MDAY:       v1 = integer(v1.(date).val.Day())
				case FN_WDAY:       v1 = integer(v1.(date).val.Weekday())
				case FN_HOUR:       v1 = integer(v1.(date).val.Hour())
				case FN_MONTHNAME:  v1 = text(v1.(date).val.Month().String())
				case FN_WDAYNAME:   v1 = text(v1.(date).val.Weekday().String())
				}
			//aggregate functions
			} else {
				index := n.tok2.(int)
				//first entry to aggragate target
				if q.toRow[index] == nil { q.toRow[index] = null("") }
				if _,ok := q.toRow[index].(null); ok {
					switch n.tok1.(int) {
					case FN_COUNT: q.toRow[index] = float(1)
					case FN_AVG:   q.toRow[index] = AverageVal{v1, 1}
					default: q.toRow[index] = v1
					}
				//update target with new value
				} else {
					switch n.tok1.(int) {
					case FN_AVG:   fallthrough
					case FN_SUM:   q.toRow[index] = q.toRow[index].Add(v1)
					case FN_MIN:   if q.toRow[index].Greater(v1) { q.toRow[index] = v1 }
					case FN_MAX:   if q.toRow[index].Less(v1) { q.toRow[index] = v1 }
					case FN_COUNT: if _,ok:= v1.(null); !ok { q.toRow[index] = q.toRow[index].Add(float(1)) }
					}
				}
			}
		//don't let any values stay nil
		} else {
			index := n.tok2.(int)
			if q.toRow[index] == nil { q.toRow[index] = null("") }
		}
		return t1,v1

	case N_VALUE:
		//literal
		if n.tok2.(int) == 0 {
			return n.tok3.(int), n.tok1.(Value)
		//column value
		} else if n.tok2.(int) == 1 {
			var val Value
			cell := s.TrimSpace(q.fromRow[n.tok1.(int)])
			if s.ToLower(cell) == "null" || cell == ""  { return n.tok3.(int), null("") }
			switch n.tok3.(int) {
				case T_INT:      a,_ := Atoi(cell);            val = integer(a)
				case T_FLOAT:    a,_ := ParseFloat(cell,64);   val = float(a)
				case T_DATE:     a,_ := d.ParseAny(cell);      val = date{a}
				case T_DURATION: a,_ := parseDuration(cell);   val = duration{a}
				case T_NULL:   val = null(cell)
				case T_STRING: val = text(cell)
			}
			if val == nil { val = null("") }
			return n.tok3.(int), val
		}

	case N_EXPRNEG:
		t1,v1 := execExpression(q, n.node1)
		if _,ok := n.tok1.(int); ok { v1 = v1.Mult(integer(-1)) }
		return t1,v1

	case N_EXPRMULT:
		t1,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			_,v2 := execExpression(q, n.node2)
			switch op {
			case SP_STAR:  v1=v1.Mult(v2)
			case SP_DIV:   v1=v1.Div(v2)
			case SP_MOD:   v1=v1.Mod(v2)
			case SP_CARROT:v1=v1.Pow(v2)
			}
		}
		return t1,v1

	case N_EXPRADD:
		t1,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			_,v2 := execExpression(q, n.node2)
			switch op {
			case SP_PLUS:   v1=v1.Add(v2)
			case SP_MINUS:  v1=v1.Sub(v2)
			}
		}
		return t1,v1

	case N_EXPRCASE:
		switch n.tok2.(int) {
		//case predicate list
		case KW_WHEN:
			t1,v1 := execCasePredList(q, n.node1)
			if t1==-1 && n.node3!=nil { return execExpression(q, n.node3) }
			if t1==-1 { return 0,null("") }
			return t1,v1
		//case expression list
		case N_EXPRADD:
			_,v1 := execExpression(q, n.node1)
			t2,v2 := execCaseExprList(q, n.node2, v1)
			if t2==-1 && n.node3!=nil { return execExpression(q, n.node3) }
			if t2==-1 { return 0,null("") }
			return t2,v2
		}
	}
	return 0,null("")
}

func execCasePredList(q *QuerySpecs, n *Node) (int,Value) {
	if n==nil { return -1,null("") }
	switch n.label {
	case N_CPREDLIST:
		typ, v1 := execCasePredList(q, n.node1)
		if typ == -1 { return execCasePredList(q, n.node2) }
		return typ,v1
	case N_CPRED:
		if evalPredicates(q,n.node1) { return execExpression(q, n.node2) }
	}
	return -1,null("")
}

func execCaseExprList(q *QuerySpecs, n *Node, testVal Value) (int,Value) {
	if n==nil { return -1,null("") }
	switch n.label {
	case N_CWEXPRLIST:
		typ, v1 := execCaseExprList(q, n.node1, testVal)
		if typ==-1 { return execCaseExprList(q, n.node2, testVal) }
		return typ,v1
	case N_CWEXPR:
		_,v1 := execExpression(q, n.node1)
		if v1.Equal(testVal) { return execExpression(q, n.node2) }
	}
	return -1,null("")
}

func evalPredicates(q *QuerySpecs, n *Node) bool {
	var negate int
	var match bool
	if n.tok2.(int) == 1 { negate ^= 1 }
	switch n.label {
	case N_PREDICATES:
		match = evalPredicates(q,n.node1)
		if n.node2 != nil {
			switch n.tok1.(int) {
			case KW_AND: if match  { match = evalPredicates(q,n.node2) }
			case KW_OR:  if !match { match = evalPredicates(q,n.node2) }
			case KW_XOR:
				match2 := evalPredicates(q,n.node2)
				match = (match && !match2) || (!match && match2)
			}
		}
		if negate==1 { return !match }
		return match

	case N_PREDCOMP:
		_,expr1 := execExpression(q, n.node1)
		_,expr2 := execExpression(q, n.node2)
		switch n.tok1.(int) {
		case KW_LIKE:    match = expr2.Equal(expr1)
		case SP_NOEQ:    negate ^= 1; fallthrough
		case SP_EQ:      match = expr1.Equal(expr2)
		case SP_LESSEQ:  match = expr1.LessEq(expr2)
		case SP_GREAT:   match = expr1.Greater(expr2)
		case SP_GREATEQ: match = expr1.GreatEq(expr2)
		case SP_LESS:    match = expr1.Less(expr2)
		case KW_BETWEEN:
			if _,ok:=expr1.(null);ok{return false}
			if _,ok:=expr2.(null);ok{return false}
			_,expr3 := execExpression(q, n.node3)
			if _,ok:=expr3.(null);ok{return false}
			if expr1.GreatEq(expr2) {
				match = expr1.Less(expr3)
			} else {
				match = expr1.GreatEq(expr3)
			}
		}
	}
	if negate==1 { return !match }
	return match
}
