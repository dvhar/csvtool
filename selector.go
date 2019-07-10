package main
import (
	. "strconv"
	d "github.com/araddon/dateparse"
	s "strings"
	//. "fmt"
)

//return bool for keep going or reached limit
func execSelect(q *QuerySpecs, res*SingleQueryResult) {

	execSelections(q, q.tree.node1.node1)

	//add non-grouped row to web return data
	if q.quantityRetrieved <= q.showLimit && !q.groupby {
		res.Vals = append(res.Vals, q.toRow)
		q.quantityRetrieved++
	}

	//save non-grouped row
	if q.save && !q.groupby { saver <- saveData{Type : CH_ROW, Row : &q.toRow} ; <-savedLine}

}

func execSelections(q *QuerySpecs, n *Node) {
	if n == nil { return }
	index := n.tok1.(int)
	typ,val := execExpression(q, n.node1.node1)
	if val == nil { val = null("") }
	if typ != T_AGGRAGATE{
		q.toRow[index] = val.(Value)
	} else if val.(Aggragate).val != nil {
		v := val.(Aggragate).val.(Value)
		//first entry to aggragate target
		if q.toRow[index] == nil {
			switch val.(Aggragate).function {
			case FN_COUNT: q.toRow[index] = integer(1)
			case FN_AVG:   q.toRow[index] = AverageVal{v, 1}
			default: q.toRow[index] = v
			}
		//update target with new value
		} else {
			switch val.(Aggragate).function {
			case FN_AVG:   fallthrough
			case FN_SUM:   q.toRow[index] = q.toRow[index].Add(v)
			case FN_MIN:   if q.toRow[index].Greater(v) { q.toRow[index] = v }
			case FN_MAX:   if q.toRow[index].Less(v) { q.toRow[index] = v }
			case FN_COUNT: q.toRow[index] = q.toRow[index].Add(integer(1))
			}
		}
	}
	execSelections(q, n.node2)
}

func evalWhere(q *QuerySpecs) bool {
	node := q.tree.node3
	if node.node1 == nil { return true }
	return evalPredicates(q, node.node1)
}

//set target row and return bool for valid row
func execGroupOrNewRow(q *QuerySpecs, n *Node) bool {
	//not grouping
	if !q.groupby { q.toRow = make([]Value, q.colSpec.NewWidth); q.quantityRetrieved++; return true }

	//grouping to a single row because no groupby clause
	if n == nil {
		if q.toRow == nil { q.toRow = make([]Value, q.colSpec.NewWidth); q.quantityRetrieved++ }
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
		} else if !q.LimitReached() {
			q.quantityRetrieved++
			row = make([]Value, q.colSpec.NewWidth)
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
func execExpression(q *QuerySpecs, n *Node) (int,interface{}) {
	switch n.label {
	case N_FUNCTION:
		t1,v1 := execExpression(q, n.node1)
		functionId := n.tok1.(int)
		//aggregate function
		if (functionId & AGG_BIT) != 0 { return T_AGGRAGATE, Aggragate{v1,t1,functionId} }
		//non-aggregate function
		if v1 != nil {
			switch functionId {
			case FN_ABS:   if v1.(Value).Less(integer(0)) { v1 = v1.(Value).Mult(integer(-1)) }
			case FN_YEAR:  v1 = integer(v1.(date).val.Year())
			case FN_MONTH: v1 = integer(v1.(date).val.Month())
			case FN_WEEK:  v1 = integer(v1.(date).val.YearDay() / 7)
			case FN_YDAY:  v1 = integer(v1.(date).val.YearDay())
			case FN_MDAY:  v1 = integer(v1.(date).val.Day())
			case FN_WDAY:   v1 = integer(v1.(date).val.Weekday())
			case FN_HOUR:  v1 = integer(v1.(date).val.Hour())
			case FN_MONTHNAME: v1 = text(v1.(date).val.Month().String())
			case FN_WDAYNAME:   v1 = text(v1.(date).val.Weekday().String())
			}
		}
		return t1,v1

	case N_VALUE:
		//literal
		if n.tok2.(int) == 0 {
			return n.tok3.(int), n.tok1
		//column value
		} else if n.tok2.(int) == 1 {
			var val Value
			cell := s.TrimSpace(q.fromRow[n.tok1.(int)])
			if s.ToLower(cell) == "null" || cell == ""  { return n.tok3.(int), nil }
			switch n.tok3.(int) {
				case T_INT:	     a,_ := Atoi(cell);            val = integer(a)
				case T_FLOAT:    a,_ := ParseFloat(cell,64);   val = float(a)
				case T_DATE:     a,_ := d.ParseAny(cell);      val = date{a}
				case T_DURATION: a,_ := parseDuration(cell); val = duration{a}
				case T_NULL:   val = nil
				case T_STRING: val = text(cell)
			}
			return n.tok3.(int), val
		}

	case N_EXPRNEG:
		t1,v1 := execExpression(q, n.node1)
		if _,ok := n.tok1.(int); ok && v1 != nil { v1 = v1.(Value).Mult(integer(-1)) }
		return t1,v1

	case N_EXPRMULT:
		t1,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			if v1 == nil { return t1,v1 }
			_,v2 := execExpression(q, n.node2)
			if v2 == nil { return t1,v2 }
			switch op {
			case SP_STAR: v1=v1.(Value).Mult(v2.(Value))
			case SP_DIV:  v1=v1.(Value).Div(v2.(Value))
			case SP_MOD:  v1=v1.(Value).Mod(v2.(Value))
			}
		}
		return t1,v1

	case N_EXPRADD:
		t1,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			if v1 == nil { return t1,v1 }
			_,v2 := execExpression(q, n.node2)
			if v2 == nil { return t1,v2 }
			switch op {
			case SP_PLUS:   v1=v1.(Value).Add(v2.(Value))
			case SP_MINUS:  v1=v1.(Value).Sub(v2.(Value))
			}
		}
		return t1,v1

	//optimizer means N_EXPRCASE is only present if actual case statement
	case N_EXPRCASE:
		switch n.tok2.(int) {
		//case predicate list
		case KW_WHEN:
			t1,v1 := execCasePredList(q, n.node1)
			if t1==-1 && n.node3!=nil { return execExpression(q, n.node3) }
			if t1==-1 { return 0,nil }
			return t1,v1
		//case expression list
		case N_EXPRADD:
			_,v1 := execExpression(q, n.node1)
			t2,v2 := execCaseExprList(q, n.node2, v1)
			if t2==-1 && n.node3!=nil { return execExpression(q, n.node3) }
			if t2==-1 { return 0,nil }
			return t2,v2
		}
	}
	return 0,nil
}

func execCasePredList(q *QuerySpecs, n *Node) (int,interface{}) {
	if n==nil { return -1,nil }
	switch n.label {
	case N_CPREDLIST:
		typ, v1 := execCasePredList(q, n.node1)
		if typ == -1 { return execCasePredList(q, n.node2) }
		return typ,v1
	case N_CPRED:
		if evalPredicates(q,n.node1) { return execExpression(q, n.node2) }
	}
	return -1,nil
}

func execCaseExprList(q *QuerySpecs, n *Node, testVal interface{}) (int,interface{}) {
	if n==nil { return -1,nil }
	switch n.label {
	case N_CWEXPRLIST:
		typ, v1 := execCaseExprList(q, n.node1, testVal)
		if typ==-1 { return execCaseExprList(q, n.node2, testVal) }
		return typ,v1
	case N_CWEXPR:
		_,v1 := execExpression(q, n.node1)
		if (v1 != nil && testVal != nil && v1.(Value).Equal(testVal.(Value))) ||
			v1 == nil && testVal == nil { return execExpression(q, n.node2) }
	}
	return -1,nil
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
		_,val1 := execExpression(q, n.node1)
		_,val2 := execExpression(q, n.node2)
		if val1 == nil || val2 == nil {
			switch n.tok1.(int) {
			case SP_NOEQ: negate ^= 1; fallthrough
			case SP_EQ:  match = (val1 == nil && val2 == nil)
			default: match = false
			}
		} else {
			expr1 := val1.(Value)
			expr2 := val2.(Value)
			switch n.tok1.(int) {
			case KW_LIKE:    match = expr2.Equal(expr1)
			case SP_NOEQ:    negate ^= 1; fallthrough
			case SP_EQ:      match = expr1.Equal(expr2)
			case SP_LESSEQ:  match = expr1.LessEq(expr2)
			case SP_GREAT:   match = expr1.Greater(expr2)
			case SP_GREATEQ: match = expr1.GreatEq(expr2)
			case SP_LESS:    match = expr1.Less(expr2)
			case KW_BETWEEN:
				_,val3 := execExpression(q, n.node3)
				if val3 == nil { match = false; break }
				expr3 := val3.(Value)
				if expr1.GreatEq(expr2) {
					match = expr1.Less(expr3)
				} else {
					match = expr1.GreatEq(expr3)
				}
			}
		}
	}
	if negate==1 { return !match }
	return match
}
