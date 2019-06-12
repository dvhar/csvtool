package main
import (
  . "fmt"
  . "strconv"
  d "github.com/araddon/dateparse"
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

//predicate typing and enforcing needs work
//type checker
//only return val interface if can be precomputed
var caseWhenExprType int
var it int
func typeCheck(n *Node) (int, int, interface{}, error) {  //returns nodetype, datatype, value(if literal), err
	if n == nil { return 0,0,nil,nil }

	//printer for debugging
	it++
	for j:=0;j<it;j++ { Print("  ") }
	Println(treeMap[n.label])
	for j:=0;j<it;j++ { Print("  ") }
	Println("toks:",n.tok1, n.tok2, n.tok3)
	defer func(){ it-- }()

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
	case N_SELECTIONS:
		_, d1, v1, err := typeCheck(n.node1)
		if err != nil { return 0,0,nil,err }
		switch n.label {
		case N_EXPRNEG:
			if _,ok:=n.tok1.(int); ok && d1 != T_INT && d1 != T_FLOAT {
				Println("minus error");
				err = errors.New("Minus sign does not work with type "+typeMap[d1]) }
		case N_COLITEM:
			n.tok3 = d1
			Println("n_colitem is type",d1," value ",v1)
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

			//check addition semantics
			if n.label==N_EXPRADD && !isOneOfType(d1,d2,T_INT,T_FLOAT) && !(d1==T_STRING && d2==T_STRING){
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
	var err error
	var val interface{}
	switch n.label {
	case N_VALUE:
		n.tok3 = t
		if n.tok2 == 0 {
			if _,ok := n.tok1.(*regexp.Regexp); ok { return err }
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
		if tk2,ok := n.tok2.(int); ok && tk2 == N_EXPRADD {
			err = enforceType(n.node1, n.node2.tok3.(int))  //initial when expression
			if err != nil { return err }
			for whenNode := n.node2; whenNode.node2 != nil; whenNode = whenNode.node2 {  //when expression list
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

//remove useless nodes from parse tree, give column names if applicable
func branchShortener(q *QuerySpecs, n *Node) *Node {
	if n == nil { return n }
	n.node1 = branchShortener(q, n.node1)
	n.node2 = branchShortener(q, n.node2)
	n.node3 = branchShortener(q, n.node3)
	//node only links to next node
	if n.tok1 == nil &&
		n.tok2 == nil &&
		n.tok3 == nil &&
		n.node2 == nil &&
		n.node3 == nil { return n.node1 }
	//predicates has no logical operator so it's just one predicate
	if n.label == N_PREDICATES && n.tok1 == nil { return n.node1 }
	//case node just links to next node
	if n.label == N_EXPRCASE &&
		(n.tok1.(int) == WORD || n.tok1.(int) == N_EXPRADD) { return n.node1 }
	//give colitem name of source column if just a col
	if n.label == N_COLITEM &&
		n.tok1 == nil &&
		n.node1.label == N_VALUE &&
		n.node1.tok2.(int) == 1 { n.tok1 = q.files["_fmk01"].names[n.node1.tok1.(int)] }
	if n.label == N_SELECTIONS &&
		n.node1.label == N_COLITEM {
			if n.node1.tok1 != nil { n.tok2 = n.node1.tok1
			} else { n.tok2 = Sprintf("col%d",n.tok1.(int)) }
			newColItem(q, n.tok1.(int), n.node1.tok3.(int), n.tok2.(string))
		}
	return n
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
