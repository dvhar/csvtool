package main
import (
  . "fmt"
  . "strconv"
  d "github.com/araddon/dateparse"
  "regexp"
  "errors"
)

func evalWhere(q *QuerySpecs) bool {
	node := q.tree.node3
	if node.node1 == nil { return true }
	return evalPredicates(q, node.node1)
}

//what type results from operation with 2 expressions with various data types and column/literal source
//null[c,l], int[c,l], float[c,l], date[c,l], string[c,l] in both dimensions
var typeChart = [10][10]int {
	{4,4,4,4,4,4,4,4,4,4},
	{4,4,1,1,2,2,3,3,4,4},
	{4,1,1,1,2,1,3,1,4,1},
	{4,1,1,1,2,2,3,1,4,4},
	{4,2,2,2,2,2,3,2,4,2},
	{4,2,1,2,2,2,3,2,4,4},
	{4,3,3,3,3,3,3,3,3,3},
	{4,3,1,1,2,2,3,3,4,4},
	{4,4,4,4,4,4,3,4,4,4},
	{4,4,1,4,2,4,3,4,4,4},
}

//figure out what type to give subtreee given its expression types
//value being null or not depends on csv column value or literal
func typeCompute(v1, v2, v3 interface{}, d1, d2, d3, howmany int) int {
	i1 := 2*d1
	i2 := 2*d2
	if v1 != nil { i1++ }
	if v2 != nil { i2++ }
	resultType := typeChart[i1][i2]
	if howmany == 3 {
		i3 := 2*d3
		if v3 != nil { i3++ }
		resultType *= 2
		if v1!=nil && v2!=nil { resultType++ }
		resultType = typeChart[resultType][i3]
	}
	return resultType
}

//type checker
//only return val interface if can be precomputed
var it int
var caseWhenExprType int
func typeCheck(n *Node) (int, int, interface{}, error) {  //returns nodetype, datatype, value(if literal), err
	if n == nil { return 0,0,nil,nil }

	//printer for debugging
	if db.verbose3 {
		it++
		for j:=0;j<it;j++ { Print("  ") }
		Println(treeMap[n.label])
		for j:=0;j<it;j++ { Print("  ") }
		Println("toks:",n.tok1, n.tok2, n.tok3)
		defer func(){ it-- }()
	}

	var val interface{}
	switch n.label {

	case N_VALUE:
		if n.tok2.(int)==0 { val = n.tok1 }
		return N_VALUE, n.tok3.(int), val, nil

	case N_EXPRCASE:
		switch n.tok1.(int) {
		case WORD:  fallthrough
		case N_EXPRADD:
			return typeCheck(n.node1)
		case KW_CASE:
			n1, d1, v1, err := typeCheck(n.node1)
			if err != nil { return 0,0,nil,err }
			_, d2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			n3, d3, v3, err := typeCheck(n.node3)
			if err != nil { return 0,0,nil,err }
			var thisType int
			//when comparing an intial expression
			if n1 == N_EXPRADD {
				//independent type cluster is n.node1 and n.node2.node1.node1, looping with n=n.node2
				caseWhenExprType := d1
				for whenNode := n.node2; whenNode.node2 != nil; whenNode = whenNode.node2 {  //when expression list
					_,whentype,_,err := typeCheck(whenNode.node1.node1)
					if err != nil { return 0,0,nil,err }
					caseWhenExprType = typeCompute(nil,nil,nil,caseWhenExprType,whentype,0,2)
				}
				n.node2.tok3 = caseWhenExprType
				thisType = d2
				if n3>0 { thisType = typeCompute(v2,v3,nil,d2,d3,0,2) }
			//when using predicates
			} else {
				thisType = d1
				if n3>0 { thisType = typeCompute(v1,v3,nil,d1,d3,0,2) }
			}
			if n1 == N_VALUE { val = v1 }
			return N_EXPRCASE, thisType, val, nil
		}

	//1 or 2 type-independant nodes
	case N_EXPRNEG:   fallthrough
	case N_COLITEM:   fallthrough
	case N_WHERE:     fallthrough
	case N_SELECTIONS:
		_, d1, v1, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		switch n.label {
		case N_EXPRNEG:
			if _,ok:=n.tok1.(int); ok && d1 != T_INT && d1 != T_FLOAT {
				Println("minus error");
				err = errors.New("Minus sign does not work with type "+typeMap[d1]) }
		case N_COLITEM: n.tok3 = d1; fallthrough
		case N_WHERE:
			err = enforceType(n.node1, d1)
		case N_SELECTIONS: _, _, _, err = typeCheck(n.node2)
		}
		return n.label, d1, v1, err

	//1 2 or 3 type-interdependant nodes
	case N_EXPRADD:   fallthrough
	case N_EXPRMULT:  fallthrough
	case N_CWEXPRLIST:fallthrough
	case N_CPREDLIST: fallthrough
	case N_PREDCOMP:
		n1, d1, val, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		if n.label == N_PREDCOMP && n1 == N_PREDICATES { return n.label, 0, nil,err }
		thisType := d1

		//there is second part but not a third
		if operator,ok := n.tok1.(int); ok && operator!=KW_BETWEEN {
			_, d2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			thisType = typeCompute(val,v2,nil,d1,d2,0,2)
			db.Print1("combine vals",val,v2,"types",d1,d2,"to get",thisType)

			//check addition semantics
			if n.label==N_EXPRADD && !isOneOfType(d1,d2,T_INT,T_FLOAT) && !(thisType==T_STRING) {
				Println("add error");
				return 0,0,nil, errors.New("Cannot add or subtract types "+typeMap[d1]+" and "+typeMap[d2]) }
			//check multiplication semantics
			if n.label==N_EXPRMULT && !isOneOfType(d1,d2,T_INT,T_FLOAT){
				Println("mult error");
				return 0,0,nil, errors.New("Cannot multiply or divide type "+typeMap[thisType]) }
			if v2==nil {val = nil} //TODO: precompute if possible
		}

		//there is third part because between
		if operator,ok := n.tok1.(int); ok && operator == KW_BETWEEN {
			_, d2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			_, d3, v3, err := typeCheck(n.node3)
			if err != nil { return 0,0,nil,err }
			thisType = typeCompute(val,v2,v3,d1,d2,d3,3)
			db.Print1("combine vals",val,v2,v3,"types",d1,d2,d3,"to get",thisType)
			if v2==nil&&v3==nil {val = nil} //TODO: precompute if possible
		}
		//predicate comparisions are typed independantly, so leave type in node3
		if n.label == N_PREDCOMP { n.tok3 = thisType }
		//Println(treeMap[n.label],"returning val",val)
		return n.label, thisType, val, err

	//case 'when' expression needs to match others but isn't node's return type
	case N_CWEXPR:
		//may want to precompute n.node1
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

	case N_SELECT: return typeCheck(n.node1)
	}
	return 0,0,nil,nil
}

//parse subtree values as a type
//modify this to handle 'like' with regular expressions
func enforceType(n *Node, t int) error {
	if n == nil { return nil }
	//db.Print2("enforcer at node",treeMap[n.label])
	var err error
	var val interface{}
	switch n.label {
	case N_VALUE:
		n.tok3 = t
		if n.tok2 == 0 {
			if _,ok := n.tok1.(*regexp.Regexp); ok { return err } //don't retype regex
			Println("typing tok",n.tok1,"as",t)
			switch t {
			case T_INT:
				val,err = Atoi(n.tok1.(string))
				if err != nil { return errors.New("Could not parse "+n.tok1.(string)+" as integer") }
			case T_FLOAT:
				val,err = ParseFloat(n.tok1.(string),64)
				if err != nil { return errors.New("Could not parse "+n.tok1.(string)+" as floating point number") }
			case T_DATE:
				val,err = d.ParseAny(n.tok1.(string))
				if err != nil { return errors.New("Could not parse "+n.tok1.(string)+" as date") }
			default: val = n.tok1
			}
			n.tok1 = val
		}
		return err

	case N_EXPRCASE:
		if tk2,ok := n.tok2.(int); ok && tk2 == N_EXPRADD { //initial when expression
			db.Print2("case with expression compare")
			err = enforceType(n.node1, n.node2.tok3.(int))
			if err != nil { return err }
			for whenNode := n.node2; whenNode != nil; whenNode = whenNode.node2 {  //when expression list
				db.Print2("giving exprcase type",n.node2.tok3)
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

	default:
		//predicate comparisons are typed independantly by node3
		if n.label==N_PREDCOMP {if tt, ok := n.tok3.(int); ok { t = tt }}
		err = enforceType(n.node1,t)
		if err != nil { return err }
		err = enforceType(n.node2,t)
		if err != nil { return err }
		err = enforceType(n.node3,t)
	}
	return err
}

//remove useless nodes from parse tree
func branchShortener(q *QuerySpecs, n *Node) *Node {
	colIdx=0
	if n == nil { return n }
	//db.Print2("optimizer at node",treeMap[n.label])
	n.node1 = branchShortener(q, n.node1)
	n.node2 = branchShortener(q, n.node2)
	n.node3 = branchShortener(q, n.node3)
	//node only links to next node
	if n.tok1 == nil &&
		n.tok2 == nil &&
		n.tok3 == nil &&
		n.node2 == nil &&
		n.node3 == nil &&
		n.label != N_SELECTIONS{ return n.node1 }
	//predicates has no logical operator so it's just one predicate
	if n.label == N_PREDICATES && n.tok1 == nil { return n.node1 }
	//case node just links to next node
	if n.label == N_EXPRCASE &&
		(n.tok1.(int) == WORD || n.tok1.(int) == N_EXPRADD) { return n.node1 }
	//give node its name if just a column
	if n.label == N_COLITEM &&
		n.tok1 == nil &&
		n.node1.label == N_VALUE &&
		n.node1.tok2.(int) == 1 { n.tok1 = q.files["_fmk01"].names[n.node1.tok1.(int)] }
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
	//db.Print2("colnamer node",treeMap[n.label],n)
	if n.label == N_SELECTIONS &&
		n.node1.label == N_COLITEM {
			n.tok1 = colIdx
			colIdx++
			if n.node1.tok1 != nil { n.tok2 = n.node1.tok1
			} else { n.tok2 = Sprintf("col%d",n.tok1.(int)) }
			newColItem(q, n.tok1.(int), n.node1.tok3.(int), n.tok2.(string))
		}
	columnNamer(q, n.node1)
	columnNamer(q, n.node2)
	columnNamer(q, n.node3)
}

func newColItem(q* QuerySpecs, idx, typ int, name string) {
	//db.Print2("adding new column",name)
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
	//for j:=0;j<i;j++ { Print("  ") }
	//Println("nodes:",n.node1, n.node2, n.node3)
	treePrint(n.node1,i+1)
	treePrint(n.node2,i+1)
	treePrint(n.node3,i+1)
}

//tree node labels for debugging
var treeMap = map[int]string {
	N_QUERY:      "N_QUERY",
	N_SELECT:     "N_SELECT",
	N_SELECTIONS: "N_SELECTIONS",
	N_FROM:       "N_FROM",
	N_WHERE:      "N_WHERE",
	N_ORDER:      "N_ORDER",
	N_COLITEM:    "N_COLITEM",
	N_EXPRADD:    "N_EXPRADD",
	N_EXPRMULT:   "N_EXPRMULT",
	N_EXPRNEG:    "N_EXPRNEG",
	N_CPREDLIST:  "N_CPREDLIST",
	N_CPRED:      "N_CPRED",
	N_PREDICATES: "N_PREDICATES",
	N_PREDCOMP:   "N_PREDCOMP",
	N_CWEXPRLIST: "N_CWEXPRLIST",
	N_CWEXPR:     "N_CWEXPR",
	N_EXPRCASE:   "N_EXPRCASE",
	N_VALUE:      "N_VALUE",
}
var typeMap = map[int]string {
	T_NULL:      "null",
	T_INT:       "integer",
	T_FLOAT:     "float",
	T_DATE:      "date",
	T_STRING:    "string",
}
