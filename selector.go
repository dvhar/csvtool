package main
import (
	. "fmt"
  . "strconv"
  d "github.com/araddon/dateparse"
  s "strings"
	"regexp"
	"time"
)

func execSelect(q *QuerySpecs, res*SingleQueryResult) {
	//row is target of aggregate function
	if q.groupby {
		q.toRow = execGroupBy(q,q.tree.node4)
	//normal target row
	} else {
		q.toRow = make([]interface{}, q.colSpec.NewWidth)
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
	if typ != T_AGGRAGATE{
		q.toRow[index] = val
	} else {
		v := val.(Aggragate).val
		//first entry to aggragate target
		if q.toRow[index] == nil {
			switch val.(Aggragate).function {
			case FN_COUNT: q.toRow[index] = 1
			case FN_AVG:   q.toRow[index] = AggValue{val.(Aggragate).val, 1}
			default: q.toRow[index] = v
			}
		//update target with new value
		} else if v != nil {
			switch val.(Aggragate).function {
			case FN_AVG:
				//find a better way to update member of struct interface
				count := q.toRow[index].(AggValue).count + 1
				var sum interface{}
				switch val.(Aggragate).typ {
				case T_INT:    sum = q.toRow[index].(AggValue).val.(int) + v.(int)
				case T_FLOAT:  sum = q.toRow[index].(AggValue).val.(float64) + v.(float64)
				}
			   q.toRow[index] = AggValue{sum,count}
			case FN_SUM:
				switch val.(Aggragate).typ {
				case T_INT:    q.toRow[index] = q.toRow[index].(int)     + v.(int)
				case T_FLOAT:  q.toRow[index] = q.toRow[index].(float64) + v.(float64) 
				}
			case FN_MIN:
				switch val.(Aggragate).typ {
				case T_STRING: if q.toRow[index].(string)        > v.(string)     { q.toRow[index] = v }
				case T_INT:    if q.toRow[index].(int)           > v.(int)        { q.toRow[index] = v }
				case T_FLOAT:  if q.toRow[index].(float64)       > v.(float64)    { q.toRow[index] = v }
				case T_DATE:   if q.toRow[index].(time.Time).After(v.(time.Time)) { q.toRow[index] = v }
				}
			case FN_MAX:
				switch val.(Aggragate).typ {
				case T_STRING: if q.toRow[index].(string)         < v.(string)     { q.toRow[index] = v }
				case T_INT:    if q.toRow[index].(int)            < v.(int)        { q.toRow[index] = v }
				case T_FLOAT:  if q.toRow[index].(float64)        < v.(float64)    { q.toRow[index] = v }
				case T_DATE:   if q.toRow[index].(time.Time).Before(v.(time.Time)) { q.toRow[index] = v }
				}
			case FN_COUNT:
				q.toRow[index] = q.toRow[index].(int) + 1
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
func execGroupBy(q *QuerySpecs, n *Node) []interface{} {
	if !q.groupby { return nil }
	//use q.toRow if grouping without groupby clause so group all to just one row
	if n == nil {
		if q.toRow == nil { q.toRow = make([]interface{}, q.colSpec.NewWidth) }
		return q.toRow
	}
	return execGroupExpressions(q, n.node1, n.tok1.(map[interface{}]interface{}))
}
func execGroupExpressions(q *QuerySpecs, n *Node, m map[interface{}]interface{}) []interface{} {
	_, key := execExpression(q,n.node1)
	switch n.tok1.(int) {
	case 0:
		row,ok := m[key]
		if ok {
			return row.([]interface{})
		} else {
			row = make([]interface{}, q.colSpec.NewWidth)
			m[key] = row
			return row.([]interface{})
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
	if n == nil { return 0,nil }

	switch n.label {
	case N_FUNCTION:
		t1,v1 := execExpression(q, n.node1)
		return T_AGGRAGATE, Aggragate{v1,t1,n.tok1.(int)}
	case N_VALUE:
		//literal
		if n.tok2.(int) == 0 {
			return n.tok3.(int), n.tok1
		} else if n.tok2.(int) != 2 {
			var val interface{}
			cell := q.fromRow[n.tok1.(int)]
			if s.ToLower(cell) == "null" || cell == ""  { return n.tok3.(int), nil }
			switch n.tok3.(int) {
				case T_INT:	   val,_ = Atoi(cell)
				case T_FLOAT:  val,_ = ParseFloat(cell,64)
				case T_DATE:   val,_ = d.ParseAny(cell)
				case T_NULL:   val = nil
				case T_STRING: val = cell
			}
			//Printf("column %+V being retrieved as %d\n",val,n.tok3.(int))
			return n.tok3.(int), val
		}

	case N_EXPRNEG:
		t1,v1 := execExpression(q, n.node1)
		if _,ok := n.tok1.(int); ok && v1 != nil {
			switch t1 {
			case T_INT:   v1 = v1.(int) * -1
			case T_FLOAT: v1 = v1.(float64) * -1.0
			}
		}
		return t1,v1

	case N_EXPRMULT:
		t1,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			if v1 == nil { return t1,v1 }
			_,v2 := execExpression(q, n.node2)
			if v2 == nil { return t1,v2 }
			switch t1 {
			case T_INT:
				switch op {
				case SP_STAR: v1=v1.(int)*v2.(int)
				case SP_DIV:  v1=v1.(int)/v2.(int)
				case SP_MOD:  v1=v1.(int)%v2.(int)
				}
			case T_FLOAT:
				switch op {
				case SP_STAR: v1=v1.(float64)*v2.(float64)
				case SP_DIV:  v1=v1.(float64)/v2.(float64)
				}
			}
		}
		return t1,v1

	case N_EXPRADD:
		t1,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			_,v2 := execExpression(q, n.node2)
			//Printf("%+V %+V\n", v1, v2)
			if v1==nil || v2==nil { return t1,nil }
			switch t1 {
			case T_INT:   if op==SP_PLUS { v1=v1.(int)+v2.(int) } else { v1=v1.(int)-v2.(int) }
			case T_FLOAT: if op==SP_PLUS { v1=v1.(float64)+v2.(float64) } else { v1=v1.(float64)-v2.(float64) }
			case T_STRING: if op==SP_PLUS { v1=v1.(string)+v2.(string) } //else remove substring
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
		_,expr1 := execExpression(q, n.node1)
		_,expr2 := execExpression(q, n.node2)
		typ := n.tok3.(int)
		if expr1==nil || expr2==nil { typ = T_NULL }
		switch n.tok1.(int) {
		case KW_LIKE: match = expr2.(*regexp.Regexp).MatchString(Sprint(expr1))

		case SP_NOEQ: negate ^= 1; fallthrough
		case SP_EQ:
		//db.Print2("ex1, ex3:",expr1, expr2, t1,t2)
			switch typ {
			case T_DATE:   match = expr1.(time.Time).Equal(expr2.(time.Time))
			default:	   match = expr1 == expr2
			}

		case SP_LESSEQ: negate ^= 1; fallthrough
		case SP_GREAT:
			switch typ {
			case T_NULL:   match = expr1!=nil           && expr2==nil
			case T_STRING: match = expr1.(string)        > expr2.(string)
			case T_INT:    match = expr1.(int)           > expr2.(int)
			case T_FLOAT:  match = expr1.(float64)       > expr2.(float64)
			case T_DATE:   match = expr1.(time.Time).After(expr2.(time.Time))
			}

		case SP_GREATEQ: negate ^= 1; fallthrough
		case SP_LESS:
			switch typ {
			case T_NULL:   match = expr1==nil            && expr2!=nil
			case T_STRING: match = expr1.(string)         < expr2.(string)
			case T_INT:    match = expr1.(int)            < expr2.(int)
			case T_FLOAT:  match = expr1.(float64)        < expr2.(float64)
			case T_DATE:   match = expr1.(time.Time).Before(expr2.(time.Time))
			}

		case KW_BETWEEN:
			_,expr3 := execExpression(q, n.node3)
			var biggerThanFirst bool
			switch typ {
			case T_NULL:   biggerThanFirst = expr1!=nil             && expr2==nil
			case T_STRING: biggerThanFirst = expr1.(string)         >= expr2.(string)
			case T_INT:    biggerThanFirst = expr1.(int)            >= expr2.(int)
			case T_FLOAT:  biggerThanFirst = expr1.(float64)        >= expr2.(float64)
			case T_DATE:   biggerThanFirst = !expr1.(time.Time).Before(expr2.(time.Time))
			}
			if biggerThanFirst {
				switch typ {
				case T_NULL:   match = expr1==nil            && expr3!=nil
				case T_STRING: match = expr1.(string)         < expr3.(string)
				case T_INT:    match = expr1.(int)            < expr3.(int)
				case T_FLOAT:  match = expr1.(float64)        < expr3.(float64)
				case T_DATE:   match = expr1.(time.Time).Before(expr3.(time.Time))
				}
			} else {
				switch typ {
				case T_NULL:   match = expr1!=nil             && expr3==nil
				case T_STRING: match = expr1.(string)         >= expr3.(string)
				case T_INT:    match = expr1.(int)            >= expr3.(int)
				case T_FLOAT:  match = expr1.(float64)        >= expr3.(float64)
				case T_DATE:   match = !expr1.(time.Time).Before(expr3.(time.Time))
				}
			}
		}
	}
	if negate==1 { return !match }
	return match
}
