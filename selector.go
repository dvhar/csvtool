package main
import (
	. "fmt"
	"regexp"
	"time"
)

func exec2Select(q *QuerySpecs, res*SingleQueryResult) {
	q.toRow = make([]interface{}, q.colSpec.NewWidth)
	exec2Selections(q, q.tree.node1.node1)
	if q.quantityRetrieved <= q.showLimit {
		res.Vals = append(res.Vals, q.toRow)
		q.quantityRetrieved++
	}
	if q.save { saver <- saveData{Type : CH_ROW, Row : &q.toRow} ; <-savedLine}
}

func exec2Selections(q *QuerySpecs, n *Node) {
	if n == nil { return }
	_,val := execExpression(q, n.node1.node1)
	q.toRow[n.tok1.(int)] = val
	exec2Selections(q, n.node2)
}

//returns type and value
func execExpression(q *QuerySpecs, n *Node) (int,interface{}) {
	if n == nil { return 0,nil }

	switch n.label {
	case N_VALUE:
		if n.tok2.(int) == 0 { return n.tok3.(int), n.tok1
		} else {
			return n.tok3.(int), q.fromRow[n.tok1.(int)]
		}

	case N_EXPRNEG:
		typ,v1 := execExpression(q, n.node1)
		if _,ok := n.tok1.(int); ok {
			switch typ {
			case T_INT:   v1 = v1.(int) * -1
			case T_FLOAT: v1 = v1.(float64) * -1.0
			}
		}
		return typ,v1

	case N_EXPRMULT:
		typ,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			_,v2 := execExpression(q, n.node2)
			switch typ {
			case T_INT:   if op==SP_STAR { v1=v1.(int)*v2.(int) } else { v1=v1.(int)/v2.(int) }
			case T_FLOAT: if op==SP_STAR { v1=v1.(float64)*v2.(float64) } else { v1=v1.(float64)/v2.(float64) }
			}
		}
		return typ,v1

	case N_EXPRADD:
		typ,v1 := execExpression(q, n.node1)
		if op,ok := n.tok1.(int); ok {
			_,v2 := execExpression(q, n.node2)
			switch typ {
			case T_INT:   if op==SP_PLUS { v1=v1.(int)+v2.(int) } else { v1=v1.(int)-v2.(int) }
			case T_FLOAT: if op==SP_PLUS { v1=v1.(float64)+v2.(float64) } else { v1=v1.(float64)-v2.(float64) }
			case T_STRING: if op==SP_PLUS { v1=v1.(string)+v2.(string) } //else remove substring
			}
		}
		return typ,v1

	//optimizer means N_EXPRCASE is only present if actual case statement
	case N_EXPRCASE:
		switch n.tok2.(int) {
		//case predicate list
		case KW_WHEN:
			typ,v1 := execCasePredList(q, n.node1)
			if typ==-1 && n.node3!=nil { return execExpression(q, n.node3) }
			if typ==-1 { return 0,nil }
			return typ,v1
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
	if _,ok := n.tok2.(int);ok { negate ^= 1 }
	switch n.label {
	case N_PREDICATES:
		match = evalPredicates(q,n.node1)
		if n.node2 != nil {
			switch n.tok1.(int) {
			case KW_AND: if match  { match = evalPredicates(q,n.node2) }
			case KW_OR:  if !match { match = evalPredicates(q,n.node2) }
			}
		}
		if negate==1 { return !match }
		return match

	//may need special handling of nulls
	case N_PREDCOMP:
		_,expr1 := execExpression(q, n.node1)
		_,expr2 := execExpression(q, n.node2)
		typ := n.tok3.(int)
		switch n.tok1.(int) {
		case KW_LIKE: match = expr2.(*regexp.Regexp).MatchString(Sprint(expr1))

		case SP_NOEQ: negate ^= 1; fallthrough
		case SP_EQ:
			switch typ {
			case T_DATE:   match = expr1.(time.Time).Equal(expr2.(time.Time))
			default:	   match = expr1 == expr2
			}

		case SP_LESSEQ: negate ^= 1; fallthrough
		case SP_GREAT:
			switch typ {
			case T_NULL:   match = Sprint(expr1)  > Sprint(expr2)
			case T_STRING: match = expr1.(string)        > expr2.(string)
			case T_INT:    match = expr1.(int)           > expr2.(int)
			case T_FLOAT:  match = expr1.(float64)       > expr2.(float64)
			case T_DATE:   match = expr1.(time.Time).After(expr2.(time.Time))
			}

		case SP_GREATEQ: negate ^= 1; fallthrough
		case SP_LESS:
			switch typ {
			case T_NULL:   match = Sprint(expr1)   < Sprint(expr2)
			case T_STRING: match = expr1.(string)         < expr2.(string)
			case T_INT:    match = expr1.(int)            < expr2.(int)
			case T_FLOAT:  match = expr1.(float64)        < expr2.(float64)
			case T_DATE:   match = expr1.(time.Time).Before(expr2.(time.Time))
			}

		case KW_BETWEEN:
			println("this might get ugly")
		}
	}
	if negate==1 { return !match }
	return match
}
