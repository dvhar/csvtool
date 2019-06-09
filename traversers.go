package main
import (
  . "fmt"
  "regexp"
  "time"
  "errors"
)

//traverse where branch of parse tree
func evalWhere(q *QuerySpecs, fromRow *[]interface{}) (bool, error) {
	node := q.tree.node3
	if node.node1 == nil { return true,nil }
	return wTraverse(q, node.node1, fromRow)
}

//main where section traverser
func wTraverse(q *QuerySpecs, n *Node, r *[]interface{}) (bool, error) {
	if n == nil { return false,nil }

	switch n.label {
	case N_CONDITIONS:
		match, err := wTraverse(q,n.node1,r)
		if err != nil { return false, err }
		match2, err := wTraverse(q,n.node2,r)
		if err != nil { return false, err }
		if q.tempVal == KW_AND {
			match = match && match2
		} else if q.tempVal == KW_OR {
			match = match || match2
		}
		if n.tok1 == SP_NEGATE { match = !match }
		return match, err

	case N_COMPARE:
		return execRelop(n.tok1.(TreeTok), n.node1, r)

	case N_MORE:
		if n.tok1 == nil { q.tempVal = 0; return true,nil }
		match2,err := wTraverse(q,n.node1,r)
		q.tempVal = n.tok1
		return match2, err

	default:
		_,err := wTraverse(q,n.node1,r)
		if err != nil { return false, err }
		_,err = wTraverse(q,n.node2,r)
		if err != nil { return false, err }
		_,err = wTraverse(q,n.node3,r)
		if err != nil { return false, err }
	}
	return false,nil
}

//each comparison
func execRelop(c TreeTok, n *Node, r *[]interface{}) (bool, error) {
	match := false
	colVal := (*r)[c.val.(int)]
	relop := n.tok2.(TreeTok)
	compVal := n.tok3.(TreeTok)
	negate := 0
	if n.tok1 == SP_NEGATE { negate ^= 1 }

	//if neither comparison value or column are null
	if compVal.val != nil && colVal != nil {
		switch relop.id {
		case KW_LIKE:  match = compVal.val.(*regexp.Regexp).MatchString(Sprint(colVal))
		case SP_NOEQ: negate ^= 1
				   fallthrough
		case SP_EQ :
			switch compVal.dtype {
				case T_DATE:   match = compVal.val.(time.Time).Equal(colVal.(time.Time))
				default:	   match = compVal.val == colVal
			}
		case SP_LESSEQ: negate ^= 1
				   fallthrough
		case SP_GREAT :
			switch compVal.dtype {
				case T_NULL:   fallthrough
				case T_STRING: match = colVal.(string) > compVal.val.(string)
				case T_INT:	match = colVal.(int) > compVal.val.(int)
				case T_FLOAT:  match = colVal.(float64) > compVal.val.(float64)
				case T_DATE:   match = colVal.(time.Time).After(compVal.val.(time.Time))
			}
		case SP_GREATEQ : negate ^= 1
				   fallthrough
		case SP_LESS:
			switch compVal.dtype {
				case T_NULL:   fallthrough
				case T_STRING: match = colVal.(string) < compVal.val.(string)
				case T_INT:	match = colVal.(int) < compVal.val.(int)
				case T_FLOAT:  match = colVal.(float64) < compVal.val.(float64)
				case T_DATE:   match = colVal.(time.Time).Before(compVal.val.(time.Time))
			}
		}

	//if comparison value is null
	} else if compVal.val == nil {
		switch relop.id {
			case SP_NOEQ: negate ^= 1
					   fallthrough
			case SP_EQ : match = colVal == nil
			default  : return false, errors.New("Invalid operation with null: "+relop.val.(string)+". Valid operators: = != <>")
		}
	//if only column is null
	} else if compVal.val != nil && colVal == nil  {
		switch relop.id {
			case SP_NOEQ: negate ^= 1
					   fallthrough
			default: match = false
		}
	}
	if negate == 1 { match = !match }
	return match, nil
}

//select node of tree root
func execSelect(q *QuerySpecs, res*SingleQueryResult, fromRow *[]interface{}) {
	//select all if doing that
	if q.selectAll  {
		tempArr := make([]interface{}, len(*fromRow))
		copy(tempArr, *fromRow)
		if q.quantityRetrieved <= q.showLimit {
			res.Vals = append(res.Vals, tempArr)
			q.quantityRetrieved++
		}
		if q.save { saver <- saveData{Type : CH_ROW, Row : &tempArr} ; <-savedLine }
		return
	//otherwise retrieve the selected columns
	} else {
		selected := make([]interface{}, q.colSpec.NewWidth)
		execSelections(q,q.tree.node1.node1,res,fromRow,&selected)
	}
}
//selections branch of select node
func execSelections(q *QuerySpecs, n *Node, res*SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}) {
	if n.tok1 == nil {
		if q.quantityRetrieved <= q.showLimit {
			res.Vals = append(res.Vals, *selected)
			q.quantityRetrieved++
		}
		if q.save { saver <- saveData{Type : CH_ROW, Row : selected} ; <-savedLine}
		return
	} else {
		(*selected)[n.tok2.(int)] = (*fromRow)[n.tok1.(TreeTok).val.(int)]
	}
	execSelections(q,n.node1,res,fromRow,selected)
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
	i3 := 2*d3
	if v1 != nil { i1++ }
	if v2 != nil { i2++ }
	if v3 != nil { i3++ }
	resultType := typeChart[i1][i2]
	if howmany == 3 { resultType = typeChart[resultType][i3] }
	return resultType
}

//type checker
//only return val interface if can be precomputed
func typeCheck(n *Node) (int, int, interface{}, error) {  //returns nodetype, datatype, value(if literal), err
	if n == nil { return 0,0,nil,nil }
	var val interface{}
	switch n.label {

	case N_VALUE:
		if n.tok2.(int)==0 { val = n.tok1 }
		return N_VALUE, n.tok3.(int), val, nil

	case N_EXPRCASE:
		switch n.tok1.(int) {
		case WORD: fallthrough
		case N_EXPRADD:
			return typeCheck(n.node1)
		//TODO: make initial expression match whencaseexpr type, but don't return that one
		case KW_CASE:
			_, d1, v1, err := typeCheck(n.node1)
			if err != nil { return 0,0,nil,err }
			n2, d2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			n3, d3, v3, err := typeCheck(n.node3)
			if err != nil { return 0,0,nil,err }
			var thisType int
			if n2==0 && n3==0 { thisType = d1; val = v1
			} else if n2>0 && n3==0 { thisType = typeCompute(v1,v2,nil,d1,d2,0,2)
			} else if n2==0 && n3>0 { thisType = typeCompute(v1,v3,nil,d1,d3,0,2)
			} else if n2>0 && n3>0 { thisType = typeCompute(v1,v2,v3,d1,d2,d3,3) }
			Println("case types are ",d1,d2,d3,thisType)
			return N_EXPRCASE, thisType, val, nil
		}

	case N_EXPRNEG:   fallthrough
	case N_COLITEM:   fallthrough
	case N_SELECTIONS:
		_, d1, v1, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		switch n.label {
		case N_EXPRNEG:
			if _,ok:=n.tok1.(int); ok && d1 != T_INT && d1 != T_FLOAT {
				Println("minus error");
				err = errors.New("Minus sign does not work with type "+typeMap[d1]) }
		case N_COLITEM: Println("n_colitem is type",d1," value ",v1)
		case N_SELECTIONS: _, _, _, err = typeCheck(n.node2)
		}
		return n.label, d1, val, err

	case N_EXPRADD:   fallthrough
	case N_EXPRMULT:  fallthrough
	case N_CWEXPRLIST:fallthrough
	case N_CPREDLIST: fallthrough
	case N_PREDCOMP:  fallthrough
	case N_PREDICATES:
		_, d1, val, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		thisType := d1
		//there is second part but not a third
		if rel,ok := n.tok1.(int); ok && rel!=KW_BETWEEN && rel!=N_PREDICATES  {
			_, d2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			thisType = typeCompute(val,v2,nil,d1,d2,0,2)
			if n.label==N_EXPRADD{ Println("add types",d1,d2,"got",thisType) }
			if n.label==N_EXPRMULT{ Println("mult types",d1,d2,"got",thisType) }
			//check addition semantics
			if n.label==N_EXPRADD && !isOneOfType(d1,d2,T_INT,T_FLOAT) && !(d1==T_STRING && d2==T_STRING){
				Println("add error");
				return 0,0,nil, errors.New("Cannot add or subtract types "+typeMap[d1]+" and "+typeMap[d2]) }
			//check multiplication semantics
			if n.label==N_EXPRMULT && !isOneOfType(d1,d2,T_INT,T_FLOAT){
				Println("mult error");
				return 0,0,nil, errors.New("Cannot multiply or divide type "+typeMap[thisType]) }
				val = nil //TODO: precompute if possible
		}
		//there is third part because between
		if rel,ok := n.tok1.(int); ok && rel == KW_BETWEEN {
			_, d2, v2, err := typeCheck(n.node2)
			if err != nil { return 0,0,nil,err }
			_, d3, v3, err := typeCheck(n.node3)
			if err != nil { return 0,0,nil,err }
			thisType = typeCompute(val,v2,v3,d1,d2,d3,3)
			val = nil //TODO: precompute if possible
		}
		return n.label, thisType, val, err

	//TODO: make sure 'when' expression matches parent case initial expression type
	case N_CWEXPR:
		_, whenType, _, err := typeCheck(n.node1)
		if err != nil { return 0,whenType,nil,err }
		_, thisType, _, err := typeCheck(n.node2)
		return n.label, thisType, nil, err

	case N_CPRED:
		_, predType, _, err := typeCheck(n.node1)
		if err != nil { return 0,predType,nil,err }
		//run type enforcer on predicate here ^
		_, thisType, _, err := typeCheck(n.node2)
		return n.label, thisType, nil, err

	default:
		_, _, _, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		_, _, _, err = typeCheck(n.node2)
		if err != nil { return 0,0,nil,err }
		_, _, _, err = typeCheck(n.node3)
		return 0,0,nil,err
	}
	return 0,0,nil,nil
}
//parse subtree values as a type
func enforceType(q *QuerySpecs, n *Node, d int) error {
	return nil
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
	N_TOP:        "N_TOP",
	N_SELECTIONS: "N_SELECTIONS",
	N_COLUMN:     "N_COLUMN",
	N_SPECIAL:    "N_SPECIAL",
	N_FROM:       "N_FROM",
	N_WHERE:      "N_WHERE",
	N_CONDITIONS: "N_CONDITIONS",
	N_BETWEEN:    "N_BETWEEN",
	N_MORE:       "N_MORE",
	N_COMPARE:    "N_COMPARE",
	N_REL:        "N_REL",
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
