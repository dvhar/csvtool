package main
import (
	. "fmt"
  . "strconv"
  d "github.com/araddon/dateparse"
  s "strings"
	//"regexp"
	//"time"
)

func execSelect(q *QuerySpecs, res*SingleQueryResult) {
	//row is target of aggregate function
	if q.groupby {
		q.toRow = execGroupBy(q,q.tree.node4)
	//normal target row
	} else {
		q.toRow = make([]GoStringer, q.colSpec.NewWidth)
	}
	execSelections(q, q.tree.node1.node1)
	if q.quantityRetrieved <= q.showLimit && !q.groupby {
		res.Vals = append(res.Vals, q.toRow)
		q.quantityRetrieved++
	}
	if q.save && !q.groupby { saver <- saveData{Type : CH_ROW, Row : &q.toRow} ; <-savedLine}
}

func execSelections(q *QuerySpecs, n *Node) {
	if n == nil { return }
	index := n.tok1.(int)
	typ,val := execExpression(q, n.node1.node1)
	if val == nil { val = null{""} }
	if typ != T_AGGRAGATE{
		q.toRow[index] = val.(GoStringer)
	} else if val.(Aggragate).val != nil {
		v := val.(Aggragate).val.(Value)
		//first entry to aggragate target
		if q.toRow[index] == nil {
			switch val.(Aggragate).function {
			case FN_COUNT: q.toRow[index] = integer{1}
			case FN_AVG:   q.toRow[index] = AggValue{v, 1}
			default: q.toRow[index] = v
			}
		//update target with new value
		} else {
			switch val.(Aggragate).function {
			case FN_AVG:
				//find a better way to update member of struct interface
				count := q.toRow[index].(AggValue).count + 1
				sum := q.toRow[index].(AggValue).val.(Value).Add(v)
				q.toRow[index] = AggValue{sum,count}
			case FN_SUM:
				q.toRow[index] = q.toRow[index].(Value).Add(v).(Value)
			case FN_MIN:
				if q.toRow[index].(Value).Greater(v) { q.toRow[index] = v }
			case FN_MAX:
				if q.toRow[index].(Value).Less(v) { q.toRow[index] = v }
			case FN_COUNT:
				q.toRow[index] = q.toRow[index].(Value).Add(integer{1}).(Value)
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

//return target array
func execGroupBy(q *QuerySpecs, n *Node) []GoStringer {
	if !q.groupby { return nil }
	//use q.toRow if grouping without groupby clause so group all to just one row
	if n == nil {
		if q.toRow == nil { q.toRow = make([]GoStringer, q.colSpec.NewWidth) }
		return q.toRow
	}
	return execGroupExpressions(q, n.node1, n.tok1.(map[interface{}]interface{}))
}
func execGroupExpressions(q *QuerySpecs, n *Node, m map[interface{}]interface{}) []GoStringer {
	_, key := execExpression(q,n.node1)
	switch n.tok1.(int) {
	case 0:
		row,ok := m[key]
		if ok {
			return row.([]GoStringer)
		} else {
			row = make([]GoStringer, q.colSpec.NewWidth)
			m[key] = row
			return row.([]GoStringer)
		}
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
	return nil
}

//returns type and value
//need to handle null values
func execExpression(q *QuerySpecs, n *Node) (int,interface{}) {
	switch n.label {
	case N_FUNCTION:
		t1,v1 := execExpression(q, n.node1)
		return T_AGGRAGATE, Aggragate{v1,t1,n.tok1.(int)}
	case N_VALUE:
		//literal
		if n.tok2.(int) == 0 {
			return n.tok3.(int), n.tok1
		} else if n.tok2.(int) != 2 {
			var val Value
			cell := q.fromRow[n.tok1.(int)]
			if s.ToLower(cell) == "null" || cell == ""  { return n.tok3.(int), nil }
			switch n.tok3.(int) {
				case T_INT:	   a,_ := Atoi(cell); val = integer{a}
				case T_FLOAT:  a,_ := ParseFloat(cell,64); val = float{a}
				case T_DATE:   a,_ := d.ParseAny(cell); val = date{a}
				case T_NULL:   val = nil
				case T_STRING: val = text{cell}
			}
			//Printf("column %+V being retrieved as %d\n",val,n.tok3.(int))
			return n.tok3.(int), val
		}

	case N_EXPRNEG:
		t1,v1 := execExpression(q, n.node1)
		if _,ok := n.tok1.(int); ok && v1 != nil {
			switch t1 {
			case T_INT:   v1 = v1.(Value).Mult(integer{-1})
			case T_FLOAT: v1 = v1.(Value).Mult(float{-1})
			}
		}
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
		if Sprint(v1) == Sprint(testVal) { return execExpression(q, n.node2) }
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

	//maybe find a less repetetive way to write this
	case N_PREDCOMP:
		_,val1 := execExpression(q, n.node1)
		_,val2 := execExpression(q, n.node2)
		if val1 == nil || val2 == nil {
			switch n.tok1.(int) {
			case SP_NOEQ: negate ^= 1; fallthrough
			case SP_EQ:  match = val1 == val2
			default: match = false
			}
		} else {
			expr1 := val1.(Value)
			expr2 := val2.(Value)
			switch n.tok1.(int) {
			case KW_LIKE: match = expr2.Equal(expr1)
			case SP_NOEQ: negate ^= 1; fallthrough
			case SP_EQ:      match = expr1.Equal(expr2)
			case SP_LESSEQ:  match = expr1.LessEq(expr2)
			case SP_GREAT:   match = expr1.Greater(expr2)
			case SP_GREATEQ: match = expr1.GreatEq(expr2)
			case SP_LESS:    match = expr1.Less(expr2)
			case KW_BETWEEN:
				_,val3 := execExpression(q, n.node3)
				expr3 := val3.(Value)
				biggerThanFirst := expr1.Greater(expr2)
				if biggerThanFirst {
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
