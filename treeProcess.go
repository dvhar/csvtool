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
func typeCompute(l1, l2 bool, t1, t2 int) int {
	i1 := 2*t1
	i2 := 2*t2
	if l1 { i1++ }
	if l2 { i2++ }
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
var caseWhenExprType int
func typeCheck(n *Node) (int, int, bool, error) {  //returns nodetype, datatype, literal, err
	if n == nil { return 0,0,false,nil }
	var literal bool

	switch n.label {

	case N_VALUE:
		if n.tok2.(int)==2 { //function
			_, t1, _, err := typeCheck(n.node1)
			if n.node1.tok1.(int) == FN_COUNT { n.tok4 = t1; t1 = T_FLOAT }
			return n.label,t1,false,err
		}
		if n.tok2.(int)==0 { literal = true } //literal
		return n.label, n.tok3.(int), literal, nil

	case N_EXPRCASE:
		switch n.tok1.(int) {
		case WORD:  fallthrough
		case N_EXPRADD:
			return typeCheck(n.node1)
		case KW_CASE:
			n1, t1, l1, err := typeCheck(n.node1)
			if err != nil { return 0,0,false,err }
			_, t2, l2, err := typeCheck(n.node2)
			if err != nil { return 0,0,false,err }
			n3, t3, l3, err := typeCheck(n.node3)
			if err != nil { return 0,0,false,err }
			var thisType int
			//when comparing an intial expression
			if n1 == N_EXPRADD {
				//independent type cluster is n.node1 and n.node2.node1.node1, looping with n=n.node2
				caseWhenExprType := t1
				for whenNode := n.node2; whenNode.node2 != nil; whenNode = whenNode.node2 {  //when expression list
					_,whentype,_,err := typeCheck(whenNode.node1.node1)
					if err != nil { return 0,0,false,err }
					caseWhenExprType = typeCompute(false,false,caseWhenExprType,whentype)
				}
				n.node2.tok3 = caseWhenExprType
				thisType = t2
				if n3>0 { thisType = typeCompute(l2,l3,t2,t3) }
			//when using predicates
			} else {
				thisType = t1
				if n3>0 { thisType = typeCompute(l1,l3,t1,t3) }
			}
			if n1 == N_VALUE { literal = l1 }
			return N_EXPRCASE, thisType, literal, nil
		}

	//1 or 2 type-independant nodes
	case N_EXPRESSIONS:fallthrough
	case N_EXPRNEG:    fallthrough
	case N_WHERE:      fallthrough
	case N_FUNCTION:   fallthrough
	case N_SELECTIONS:
		_, t1, l1, err := typeCheck(n.node1)
		if err != nil { return 0,0,false,err }
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
			if err != nil { return 0,0,false,err }
		case N_EXPRNEG:
			if _,ok:=n.tok1.(int); ok && t1 != T_INT && t1 != T_FLOAT && t1 != T_DURATION {
				err = errors.New("Minus sign does not work with type "+typeMap[t1]) }
		case N_WHERE:
			err = enforceType(n.node1, t1)
		case N_EXPRESSIONS:
			err = enforceType(n.node1, t1)
			_, _, _, err = typeCheck(n.node2)
		case N_SELECTIONS:
			n.tok5 = t1
			if n.tok3.(int) & (1<<3) == 0 {
				err = enforceType(n.node1, t1)
				if err != nil { return 0,0,false,err }
			}
			_, _, _, err = typeCheck(n.node2)
		}
		return n.label, t1, l1, err

	//1 2 or 3 type-interdependant nodes
	case N_EXPRADD:     fallthrough
	case N_EXPRMULT:    fallthrough
	case N_CWEXPRLIST:  fallthrough
	case N_CPREDLIST:   fallthrough
	case N_DEXPRESSIONS:fallthrough
	case N_PREDCOMP:
		n1, t1, l1, err := typeCheck(n.node1)
		if err != nil { return 0,0,false,err }
		if n.label == N_PREDCOMP && n1 == N_PREDICATES { return n.label, 0, false,err }
		thisType := t1

		//there is second part but not a third
		if operator,ok := n.tok1.(int); ok && operator!=KW_BETWEEN {
			_, t2, l2, err := typeCheck(n.node2)
			if err != nil { return 0,0,false,err }
			thisType = typeCompute(l1,l2,t1,t2)

			//see if using special rules for time/duration type interaction - not used in predicates
			keep, final := keepSubtreeTypes(t1,t2,operator)
			if keep {
				thisType = final
				n.tok2 = true
				err = enforceType(n.node1, t1)
				if err != nil { return 0,0,false,err }
				err = enforceType(n.node2, t2)
				if err != nil { return 0,0,false,err }
			}

			//check basic operator semantics
			if n.label == N_EXPRADD || n.label == N_EXPRMULT {
				err = checkOperatorSemantics(operator, t1, t2, l1, l2)
				if err != nil { return 0,0,false,err }
			}

			if l2==false {literal = false}
		}

		//there is third part because between - need to add type semantics check for duration interactions
		if operator,ok := n.tok1.(int); ok && operator == KW_BETWEEN {
			_, t2, l2, err := typeCheck(n.node2)
			if err != nil { return 0,0,false,err }
			_, t3, l3, err := typeCheck(n.node3)
			if err != nil { return 0,0,false,err }
			thisType = typeCompute(l1,l2,t1,t2)
			l12 := l1; if l2 == false { l12 = false }
			thisType = typeCompute(l12,l3,thisType,t3)
			if l3==false {l1 = false}
		}
		//predicate comparisions are typed independantly, so leave type in tok3
		if n.label == N_PREDCOMP { n.tok3 = thisType }
		return n.label, thisType, l1, err

	//case 'when' expression needs to match others but isn't node's return type
	case N_CWEXPR:
		_, thisType, _, err := typeCheck(n.node2)
		return n.label, thisType, false, err

	//only evalutates a boolean, subtrees are typed independantly
	case N_PREDICATES:
		_, _, _, err := typeCheck(n.node1)
		if err != nil { return 0,0,false,err }
		_, _, _, err = typeCheck(n.node2)
		return n.label, 0, false, err

	//each case predicate condition 
	case N_CPRED:
		_, _, _, err := typeCheck(n.node1)
		if err != nil { return 0,0,false,err }
		_, thisType, l1, err := typeCheck(n.node2)
		return n.label, thisType, l1, err

	case N_SELECT: fallthrough
	case N_GROUPBY: return typeCheck(n.node1)
	}
	return 0,0,false,nil
}

//aggregate semantics checker
func aggCheck(n *Node) (bool, bool, bool, error) {  //returns node, literal, aggregate, error
	if n == nil { return false, false, false, nil }
	switch n.label {
	case N_SELECT:
		return  aggCheck(n.node1)
	case N_SELECTIONS:
		_,_,_, err := aggCheck(n.node1)
		if err != nil { return true,false,false,err }
		return aggCheck(n.node2)
	case N_FUNCTION:
		if (n.tok1.(int) & AGG_BIT) != 0 { return true,false,true,nil }  //aggregate
		return aggCheck(n.node1)
	case N_VALUE:
		if n.tok2.(int)==2 { return aggCheck(n.node1) } //function
		if n.tok2.(int)==0 { return true, true, false, nil } //literal
		return true, false, false, nil //neither
	default:
			n1, l1, a1, err := aggCheck(n.node1)
			if err != nil { return true,false,true,err }
			n2, l2, a2, err := aggCheck(n.node2)
			if err != nil { return true,false,true,err }
			n3, l3, a3, err := aggCheck(n.node3)
			if err != nil { return true,false,true,err }
			if n1 && !n2 && !n3 { return n1,l1,a1,err }  //single node
			if n1 && n2 && !n3 {  //1st and 2nd nodes
				if err:=aggregateCombo(a1,a2,l1,l2);err!=nil { return true,false,false,err }
				return true, l1&&l2, a1||a2, nil
			}
			if n1 && !n2 && n3 {  //1st and 3rd nodes
				if err:=aggregateCombo(a1,a3,l1,l3);err!=nil { return true,false,false,err }
				return true, l1&&l3, a1||a3, nil
			}
			if n1 && n2 && n3 { //all 3 nodes
				if err:=aggregateCombo(a1,a2,l1,l2);err!=nil { return true,false,false,err }
				a1 = a1 || a2
				l1 = l1 && l2
				if err:=aggregateCombo(a1,a3,l1,l3);err!=nil { return true,false,false,err }
				return true, l1&&l3, a1||a3, nil
			}
	}
	return aggCheck(n.node1)
}

//parse subtree values as a type
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
			if n.tok1.(string) == "null" { val = null("")
			} else {
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
			}
			n.tok1 = val
		}
		//functions
		if n.tok2.(int) == 2 {
			if n.tok4 != nil { t = n.tok4.(int) }
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
//would like to eventually use bytecode engine, but for now just optimizes the tree a little for the traversers
func branchShortener(q *QuerySpecs, n *Node) *Node {
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
	//set 'distinct' expression and maybe hide it
	if n.label==N_SELECTIONS && n.tok3.(int)&1==1 {
		q.distinctExpr = n.node1
		if n.tok3.(int)&2!=0 { return n.node2 }
	}
	return n
}

//get column names and put in array
func columnNamer(q *QuerySpecs, n *Node) {
	if n == nil { return }
	if n.label == N_SELECTIONS {
		n.tok1 = []int{q.colSpec.NewWidth,q.colSpec.NewWidth}
		if n.tok2 == nil {
			//if just selecting column, use that name
			if n.node1.label == N_VALUE && n.node1.tok2.(int)==1 {
				n.tok2 = q.files[n.node1.tok5.(string)].names[n.node1.tok1.(int)]
			//give column numeric name if not already named
			} else {
				n.tok2 = Sprintf("col%d",n.tok1.([]int)[0]+1)
			}
		}
		newColItem(q, n.tok5.(int), n.tok2.(string), n.tok3.(int))
	}
	columnNamer(q, n.node1)
	columnNamer(q, n.node2)
	columnNamer(q, n.node3)
}

//find and record aggragate functions
//first ret is found aggregate, second is found too many
func findAggregateFunctions(q *QuerySpecs, n *Node) (bool,bool) {
	if n == nil { return false, false }
	found := false
	//tell selections node about found aggregate
	if n.label == N_SELECTIONS {
		fun := findAggregateFunction(n.node1)
		if fun != 0 { n.tok4 = fun }
		//not agg function, but returning value alongside aggregates
		if fun == 0 && q.groupby {
			n.tok1 = []int{q.colSpec.AggregateCount,n.tok1.([]int)[1]}
			q.colSpec.AggregateCount++
		}
	}
	//tell agg function node which intermediate index it has
	if n.label == N_FUNCTION && (n.tok1.(int)&AGG_BIT)!=0 {
		n.tok2 = q.colSpec.AggregateCount
		q.colSpec.AggregateCount++
		found = true
	}
	f1,e1 := findAggregateFunctions(q, n.node1)
	f2,e2 := findAggregateFunctions(q, n.node2)
	f3,e3 := findAggregateFunctions(q, n.node3)
	if found && (f1||f2||f3) { return true, true }
	return found||f1||f2||f3, e1||e2||e3
}
func findAggregateFunction(n *Node) int {
	if n == nil { return 0 }
	if n.label == N_FUNCTION && (n.tok1.(int)&AGG_BIT)!=0 { return n.tok1.(int) }
	a := findAggregateFunction(n.node1)
	if a != 0 { return a }
	a = findAggregateFunction(n.node2)
	if a != 0 { return a }
	return findAggregateFunction(n.node3)
}
func findHavingAggregates(q *QuerySpecs, selections *Node, n *Node) error {
	if n == nil { return nil }
	//found expression in predicate comparison
	if n.label == N_PREDCOMP && n.node2 != nil {
			_,t1,l1,err := typeCheck(n.node1)
			if err != nil { return err }
			_,t2,l2,err := typeCheck(n.node2)
			if err != nil { return err }
			_,t3,l3,err := typeCheck(n.node3)
			if err != nil { return err }
			a1 := findAggregateFunction(n.node1)
			a2 := findAggregateFunction(n.node2)
			a3 := findAggregateFunction(n.node3)
			thisType := typeCompute(l1,l2,t1,t2)
			if n.node3 != nil {
				l12 := l1; if l2 == false { l12 = false }
				thisType = typeCompute(l12,l3,thisType,t3)
				if !(l3 || a3>0) { return errors.New("'having' clause must be aggregates or literals") }
			}
			if !(l1 || a1>0) || !(l2 || a2>0) { return errors.New("'having' clause must be aggregates or literals") }

			enforceType(n.node1, thisType)
			enforceType(n.node2, thisType)
			enforceType(n.node3, thisType)

			n.node1 = branchShortener(q,n.node1)
			n.node2 = branchShortener(q,n.node2)
			n.node3 = branchShortener(q,n.node3)

			//add aggregate expression to selections for midrow
			a := []int{a1,a2,a3}
			for i,n := range []*Node{n.node1,n.node2,n.node3} {
				if a[i] > 0 {
					q.midExess++
					nn := selections.node1.node1
					for ; nn.node2 != nil; nn = nn.node2 {}
					nn.node2 = &Node{
						label: N_SELECTIONS,
						tok3: (1<<3)|(1<<4), // 1<<3 means selection was already processed, 1<<4 means exclude from results
						tok5: thisType,
						node1: n,
					}
				}
			}
		}

	findHavingAggregates(q, selections, n.node1)
	findHavingAggregates(q, selections, n.node2)
	findHavingAggregates(q, selections, n.node3)
	return nil
}

func newColItem(q* QuerySpecs, typ int, name string, info int) {
	if (info & (1<<4)) == 0 {
		q.colSpec.NewNames = append(q.colSpec.NewNames, name)
		q.colSpec.NewTypes = append(q.colSpec.NewTypes, typ)
	}
	q.colSpec.NewWidth++
	q.colSpec.NewPos = append(q.colSpec.NewPos, q.colSpec.NewWidth)
}

func isOneOfType(test1, test2, type1, type2 int) bool {
	return (test1 == type1 || test1 == type2) && (test2 == type1 || test2 == type2)
}

func aggregateCombo(a1,a2,l1,l2 bool) error {
	if (a1 && !(a2 || l2)) || (a2 && !(a1 || l1)) { return errors.New("Aggregates can only be in operations with other aggregates or literals") }
	return nil
}

//prepare join subtree
func joinExprFinder(q* QuerySpecs, n* Node, jfile string) string { //returns key of file referenced by expression
	if n == nil { return "" }
	switch n.label {
		case N_VALUE: if n.tok5 != nil { return n.tok5.(string) }
		case N_JOIN:
			joinExprFinder(q, n.node1, q.files[n.tok4.(string)].key)
			joinExprFinder(q, n.node2, "")
		case N_PREDCOMP:
			f1 := joinExprFinder(q, n.node1, jfile)
			f2 := joinExprFinder(q, n.node2, jfile)
			if jfile == f1 { n.tok4 = 1 }
			if jfile == f2 { n.tok4 = 2 }
			//don't need relop since join only uses =
			n.tok1 = jfile
		default:
			s := make([]string,5)
			s[0] = joinExprFinder(q, n.node1, jfile)
			s[1] = joinExprFinder(q, n.node2, jfile)
			s[2] = joinExprFinder(q, n.node3, jfile)
			s[3] = joinExprFinder(q, n.node4, jfile)
			s[4] = joinExprFinder(q, n.node5, jfile)
			for _,v := range s { if v != "" { return v } }
	}
	return ""
}

//print parse tree for debuggging
func treePrint(n *Node, i int){
	if n==nil {return}
	for j:=0;j<i;j++ { Print("  ") }
	Println(treeMap[n.label])
	for j:=0;j<i;j++ { Print("  ") }
	Println("toks:",n.tok1, n.tok2, n.tok3, n.tok4, n.tok5)
	treePrint(n.node1,i+1)
	treePrint(n.node2,i+1)
	treePrint(n.node3,i+1)
	treePrint(n.node4,i+1)
	treePrint(n.node5,i+1)
}

