package main
import (
  . "fmt"
  . "strconv"
  d "github.com/araddon/dateparse"
  "time"
  "errors"
)

//what type results from operation with 2 expressions with various data types and column/literal source
//don't use for types where different operation results in different type
//null[c,l], int[c,l], float[c,l], date[c,l], duration[c,l], string[c,l] in both dimensions
var typeChart = [12][12]int {
	{5,5, 5,5, 5,5, 5,5, 5,5, 5,5},
	{5,5, 1,1, 2,2, 3,3, 4,4, 5,5},
	{5,1, 1,1, 2,2, 3,1, 4,4, 5,1},
	{5,1, 1,1, 2,2, 3,1, 4,4, 5,5},
	{5,2, 2,2, 2,2, 3,2, 4,2, 5,2},
	{5,2, 2,2, 2,2, 3,2, 4,4, 5,5},
	{5,3, 3,3, 3,3, 3,3, 3,3, 3,3},
	{5,3, 1,1, 2,2, 3,3, 3,3, 5,5},
	{5,4, 4,4, 4,4, 3,3, 4,4, 5,4},
	{5,4, 4,4, 2,4, 3,3, 4,4, 5,5},
	{5,5, 5,5, 5,5, 3,5, 5,5, 5,5},
	{5,5, 1,5, 2,5, 3,5, 4,5, 5,5},
}
func typeCompute(v1, v2 interface{}, t1, t2 int) int {
	i1 := 2*t1
	i2 := 2*t2
	if v1 != nil { i1++ }
	if v2 != nil { i2++ }
	return typeChart[i1][i2]
}

//return preserveSubtrees and final type
func keepSubtreeTypes(t1, t2, op int) (bool,int) {
	switch op {
		case SP_STAR: fallthrough
		case SP_DIV:
			if t1 == T_DURATION && t2 == T_INT ||
			   t1 == T_DURATION && t2 == T_FLOAT ||
			   t2 == T_DURATION && t1 == T_INT ||
			   t2 == T_DURATION && t1 == T_FLOAT { return true, T_DURATION }
		case SP_MINUS:
			if t1 == T_DATE && t2 == T_DATE { return true, T_DURATION }
			fallthrough
		case SP_PLUS:
			if t1 == T_DURATION && t2 == T_DATE ||
			   t2 == T_DURATION && t1 == T_DATE { return true, T_DATE }
	}
	return false, 0
}

//type checker
//only return val if expression is a literal
var caseWhenExprType int
func typeCheck(n *Node) (int, int, interface{}, error) {  //returns nodetype, datatype, value(if literal), err
	if n == nil { return 0,0,nil,nil }
	var val interface{}
	switch n.label {

	case N_VALUE:
		if n.tok2.(int)==2 { //function
			_, t1, _, err := typeCheck(n.node1)
			if n.node1.tok1.(int) == FN_COUNT { t1 = T_INT }
			return n.label,t1,nil,err
		}
		if n.tok2.(int)==0 { val = n.tok1 } //literal
		return n.label, n.tok3.(int), val, nil

	case N_EXPRCASE:
		switch n.tok1.(int) {
		case WORD:  fallthrough
		case N_EXPRADD:
			return typeCheck(n.node1)
		case KW_CASE:
			n1, t1, v1, err := typeCheck(n.node1)
			if err != nil { return 0,0,nil,err }
			_, t2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			n3, d3, v3, err := typeCheck(n.node3)
			if err != nil { return 0,0,nil,err }
			var thisType int
			//when comparing an intial expression
			if n1 == N_EXPRADD {
				//independent type cluster is n.node1 and n.node2.node1.node1, looping with n=n.node2
				caseWhenExprType := t1
				for whenNode := n.node2; whenNode.node2 != nil; whenNode = whenNode.node2 {  //when expression list
					_,whentype,_,err := typeCheck(whenNode.node1.node1)
					if err != nil { return 0,0,nil,err }
					caseWhenExprType = typeCompute(nil,nil,caseWhenExprType,whentype)
				}
				n.node2.tok3 = caseWhenExprType
				thisType = t2
				if n3>0 { thisType = typeCompute(v2,v3,t2,d3) }
			//when using predicates
			} else {
				thisType = t1
				if n3>0 { thisType = typeCompute(v1,v3,t1,d3) }
			}
			if n1 == N_VALUE { val = v1 }
			return N_EXPRCASE, thisType, val, nil
		}

	//1 or 2 type-independant nodes
	case N_EXPRESSIONS:fallthrough
	case N_EXPRNEG:    fallthrough
	case N_COLITEM:    fallthrough
	case N_WHERE:      fallthrough
	case N_FUNCTION:   fallthrough
	case N_SELECTIONS:
		_, t1, v1, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		switch n.label {
		case N_FUNCTION:
			err := checkFunctionParamType(n.tok1.(int), t1)
			//some functions have a specific return type
			switch n.tok1.(int) {
			case FN_YEAR:  fallthrough
			case FN_MONTH: fallthrough
			case FN_WEEK:  fallthrough
			case FN_YDAY:  fallthrough
			case FN_MDAY:  fallthrough
			case FN_WDAY:   fallthrough
			case FN_HOUR:  t1 = T_INT
			case FN_MONTHNAME: fallthrough
			case FN_WDAYNAME: t1 = T_STRING
			}
			if err != nil { return 0,0,nil,err }
		case N_EXPRNEG:
			if _,ok:=n.tok1.(int); ok && t1 != T_INT && t1 != T_FLOAT && t1 != T_DURATION {
				err = errors.New("Minus sign does not work with type "+typeMap[t1]) }
		case N_COLITEM: n.tok3 = t1; fallthrough
		case N_WHERE:
			err = enforceType(n.node1, t1)
		case N_EXPRESSIONS:
			err = enforceType(n.node1, t1)
			fallthrough
		case N_SELECTIONS: _, _, _, err = typeCheck(n.node2)
		}
		return n.label, t1, v1, err

	//1 2 or 3 type-interdependant nodes
	case N_EXPRADD:   fallthrough
	case N_EXPRMULT:  fallthrough
	case N_CWEXPRLIST:fallthrough
	case N_CPREDLIST: fallthrough
	case N_PREDCOMP:
		n1, t1, v1, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		if n.label == N_PREDCOMP && n1 == N_PREDICATES { return n.label, 0, nil,err }
		thisType := t1

		//there is second part but not a third
		if operator,ok := n.tok1.(int); ok && operator!=KW_BETWEEN {
			_, t2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			thisType = typeCompute(v1,v2,t1,t2)

			//see if using special rules for time/duration type interaction
			keep, final := keepSubtreeTypes(t1,t2,operator)
			if keep {
				thisType = final
				n.tok2 = true
				err = enforceType(n.node1, t1)
				if err != nil { return 0,0,nil,err }
				err = enforceType(n.node2, t2)
				if err != nil { return 0,0,nil,err }
			}

			//check basic operator semantics
			if n.label == N_EXPRADD || n.label == N_EXPRMULT {
				err = checkOperatorSemantics(operator, t1, t2, v1, v2)
				if err != nil { return 0,0,nil,err }
			}

			if v2==nil {val = nil}
		}

		//there is third part because between - need to add type semantics check for duration interactions
		if operator,ok := n.tok1.(int); ok && operator == KW_BETWEEN {
			_, t2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			_, d3, v3, err := typeCheck(n.node3)
			if err != nil { return 0,0,nil,err }
			thisType = typeCompute(v1,v2,t1,t2)
			v12 := v1; if v2 == nil { v12 = nil }
			thisType = typeCompute(v12,v3,thisType,d3)
			if v3==nil {v1 = nil}
		}
		//predicate comparisions are typed independantly, so leave type in tok3
		if n.label == N_PREDCOMP { n.tok3 = thisType }
		return n.label, thisType, v1, err

	//case 'when' expression needs to match others but isn't node's return type
	case N_CWEXPR:
		_, thisType, _, err := typeCheck(n.node2)
		return n.label, thisType, nil, err

	//only evalutates a boolean, subtrees are typed independantly
	case N_PREDICATES:
		_, _, _, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		_, _, _, err = typeCheck(n.node2)
		return n.label, 0, nil, err

	//each predicate condition 
	case N_CPRED:
		_, _, _, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		_, thisType, v1, err := typeCheck(n.node2)
		return n.label, thisType, v1, err

	case N_SELECT: fallthrough
	case N_GROUPBY: return typeCheck(n.node1)
	}
	return 0,0,nil,nil
}

//parse subtree values as a type
//modify this to handle 'like' with regular expressions
func enforceType(n *Node, t int) error {
	if n == nil { return nil }
	//Println("enforcer at node",treeMap[n.label],"with type",t)
	var err error
	var val interface{}
	switch n.label {
	case N_VALUE:
		if n.tok1 == nil { return nil }
		n.tok3 = t
		if n.tok2.(int) == 0 {
			if _,ok := n.tok1.(liker); ok { return err } //don't retype regex
			switch t {
			case T_INT:
				val,err = Atoi(n.tok1.(string))
				if err != nil { return errors.New("Could not parse "+n.tok1.(string)+" as integer") }
				val = integer(val.(int))
			case T_FLOAT:
				val,err = ParseFloat(n.tok1.(string),64)
				if err != nil { return errors.New("Could not parse "+n.tok1.(string)+" as floating point number") }
				val = float(val.(float64))
			case T_DATE:
				val,err = d.ParseAny(n.tok1.(string))
				if err != nil { return errors.New("Could not parse "+n.tok1.(string)+" as date") }
				val = date{val.(time.Time)}
			case T_DURATION:
				val,err = parseDuration(n.tok1.(string))
				if err != nil { return errors.New("Could not parse "+n.tok1.(string)+" as time duration") }
				val = duration{val.(time.Duration)}
			case T_NULL:   val = null(n.tok1.(string))
			case T_STRING: val = text(n.tok1.(string))
			}
			n.tok1 = val
		}
		//functions
		if n.tok2.(int) == 2 {
			err = enforceType(n.node1, t)
		}
		return err

	case N_FUNCTION:
		switch n.tok1.(int) {
		case FN_YEAR:      fallthrough
		case FN_MONTH:     fallthrough
		case FN_WEEK:      fallthrough
		case FN_YDAY:      fallthrough
		case FN_MDAY:      fallthrough
		case FN_WDAY:      fallthrough
		case FN_MONTHNAME: fallthrough
		case FN_WDAYNAME:  fallthrough
		case FN_HOUR:  err = enforceType(n.node1, T_DATE)
		default: err = enforceType(n.node1, t)
		}
		return err

	case N_EXPRCASE:
		if tk2,ok := n.tok2.(int); ok && tk2 == N_EXPRADD { //initial when expression
			err = enforceType(n.node1, n.node2.tok3.(int))
			if err != nil { return err }
			for whenNode := n.node2; whenNode != nil; whenNode = whenNode.node2 {  //when expression list
				err = enforceType(whenNode.node1.node1, n.node2.tok3.(int))
				if err != nil { return err }
			}
			//finally get to this node's type
			err = enforceType(n.node2,t)
			if err != nil { return err }
		} else {
			err = enforceType(n.node1,t)
			if err != nil { return err }
		}
		err = enforceType(n.node3,t)

	//node1 already done by N_EXPRCASE whenNode loop
	case N_CWEXPR:
		err = enforceType(n.node2,t)

	//each predicate comparison has its own independant type
	case N_PREDCOMP:
		if tt, ok := n.tok3.(int); ok { t = tt }
		fallthrough
	case N_EXPRADD: fallthrough
	case N_EXPRMULT:
		//subtrees were already enforced if doing operation that preserves subtree types
		if _,ok := n.tok2.(bool); ok { return err }
		fallthrough
	default:
		err = enforceType(n.node1,t)
		if err != nil { return err }
		err = enforceType(n.node2,t)
		if err != nil { return err }
		err = enforceType(n.node3,t)
	}
	return err
}

//remove useless nodes from parse tree
//would like to eventually use bytecode engince, but for now just optimizes the tree a little for the traversers
func branchShortener(q *QuerySpecs, n *Node) *Node {
	colIdx=0
	if n == nil { return n }
	n.node1 = branchShortener(q, n.node1)
	n.node2 = branchShortener(q, n.node2)
	n.node3 = branchShortener(q, n.node3)
	//node only links to next node
	if n.tok1 == nil &&
		n.tok3 == nil &&
		n.node2 == nil &&
		n.node3 == nil &&
		n.label != N_SELECTIONS{
			if n.tok2 == nil { return n.node1 }
			//tok2 in this case is just used to preserve subtree type in previous step
			if n.label == N_EXPRADD || n.label == N_EXPRMULT { return n.node1 }
		}
	//predicates has no logical operator or negator so it's just one predicate
	if n.label == N_PREDICATES && n.tok1 == nil && n.tok2 == nil { return n.node1 }
	//case node just links to next node
	if n.label == N_EXPRCASE &&
		(n.tok1.(int) == WORD || n.tok1.(int) == N_EXPRADD) { return n.node1 }
	//value node leads to function
	if n.label == N_VALUE && n.tok2.(int) == 2 { return n.node1 }
	//give node its name if just a column
	if n.label == N_COLITEM &&
		n.tok1 == nil &&
		n.node1.label == N_VALUE &&
		n.node1.tok2.(int) == 1 { n.tok1 = q.files["_f1"].names[n.node1.tok1.(int)] }
	if t,ok := n.tok3.(int); ok && t&1==1 && n.label==N_SELECTIONS {
		q.distinctExpr = n.node1.node1
		if t&2!=0 { return n.node2 }
	}
	return n
}

//get column names and put in array
var colIdx int
func columnNamer(q *QuerySpecs, n *Node) {
	if n == nil { return }
	if n.label == N_SELECTIONS &&
		n.node1.label == N_COLITEM {
			n.tok1 = colIdx
			if n.node1.tok1 != nil { n.tok2 = n.node1.tok1
			} else { n.tok2 = Sprintf("col%d",n.tok1.(int)+1) }
			newColItem(q, n.tok1.(int), n.node1.tok3.(int), n.tok2.(string))
			colIdx++
		}
	columnNamer(q, n.node1)
	columnNamer(q, n.node2)
	columnNamer(q, n.node3)
}

//find aggragate functions that will need postprocessing
func findAggregateFunctions(q *QuerySpecs, n *Node) {
	if q.colSpec.functions == nil {q.colSpec.functions = make([]Aggragate, q.colSpec.NewWidth)}
	if n == nil { return }
	if n.label == N_SELECTIONS {
		if fun := findAggregateFunction(n.node1); fun != nil {
			q.colSpec.functions[n.tok1.(int)] = *fun
			q.colSpec.functions[n.tok1.(int)].typ = q.colSpec.NewTypes[n.tok1.(int)]
		}
	}
	findAggregateFunctions(q, n.node1)
	findAggregateFunctions(q, n.node2)
	findAggregateFunctions(q, n.node3)
}
func findAggregateFunction(n *Node) *Aggragate {
	if n == nil { return nil }
	if n.label == N_FUNCTION && (n.tok1.(int)&AGG_BIT)!=0 { return &Aggragate{nil,0,n.tok1.(int)} }
	a := findAggregateFunction(n.node1)
	if a != nil { return a }
	a = findAggregateFunction(n.node2)
	if a != nil { return a }
	return findAggregateFunction(n.node3)
}

func newColItem(q* QuerySpecs, idx, typ int, name string) {
	q.colSpec.NewNames = append(q.colSpec.NewNames, name)
	q.colSpec.NewTypes = append(q.colSpec.NewTypes, typ)
	q.colSpec.NewPos = append(q.colSpec.NewPos, idx+1)
	q.colSpec.NewWidth++
}

func isOneOfType(test1, test2, type1, type2 int) bool {
	return (test1 == type1 || test1 == type2) && (test2 == type1 || test2 == type2)
}

//print parse tree for debuggging
func treePrint(n *Node, i int){
	if n==nil {return}
	for j:=0;j<i;j++ { Print("  ") }
	Println(treeMap[n.label])
	for j:=0;j<i;j++ { Print("  ") }
	Println("toks:",n.tok1, n.tok2, n.tok3)
	treePrint(n.node1,i+1)
	treePrint(n.node2,i+1)
	treePrint(n.node3,i+1)
}

