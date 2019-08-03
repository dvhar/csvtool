/*
<query>             -> <Select> <from> <where> <groupby> <having> <orderby>
<Select>            -> {c} Select { top # } <Selections>
<Selections>        -> * <Selections> | {alias =} <exprAdd> {as alias} <Selections> | ε
<exprAdd>           -> <exprMult> ( + | - ) <exprAdd> | <exprMult>
<exprMult>          -> <exprNeg> ( * | / | % ) <exprMult> | <exprNeg>
<exprNeg>           -> { - } <exprCase>
<exprCase>          -> case <caseWhenPredList> { else <exprAdd> } end
                     | case <exprAdd> <caseWhenExprList> { else <exprAdd> } end
                     | <value>
<value>             -> column | literal | ( <exprAdd> ) | <function>
<caseWhenExprList>  -> <caseWhenExpr> <caseWhenExprList> | <caseWhenExpr>
<caseWhenExpr>      -> when <exprAdd> then <exprAdd>
<caseWhenPredList>  -> <casePredicate> <caseWhenPredList> | <casePredicate>
<casePredicate>     -> when <predicates> then <exprAdd>
<predicates>        -> <predicateCompare> { <logop> <predicates> }
<predicateCompare>  -> {not} <exprAdd> {not} <relop> <exprAdd> 
                     | {not} <exprAdd> {not} between <exprAdd> and <exprAdd>
                     | {not} ( predicates )
<function>          -> <functionname> ( <exprAdd> )
<from>              -> from filename { as alias } <joinChain>
<joinChain>         -> <join> <joinChain> | ε
<join>              -> { left | right | ε } { inner | outer | ε }
                       join file as alias on <predicates>
<where>             -> where <predicates> | ε
<having>            -> having <predicates> | ε
<groupby>           -> group by <expressions> | ε
<expressions>       -> <exprAdd> { <expressions> }
<orderby>           -> order by <exprAdd> | ε
*/


package main
import (
	"errors"
	"regexp"
	"strings"
	//"path/filepath"
	. "strconv"
	. "fmt"
	bt "github.com/google/btree"
)

//recursive descent parser builds parse tree and QuerySpecs
func parseQuery(q* QuerySpecs) (*Node,error) {
	_ = Print
	n := &Node{label:N_QUERY}
	n.tok1 = q
	//reset some global vars before parsing each query
	lineNo = 1
	err := scanTokens(q)
	if err != nil { return n,err }
	err = openFiles(q)
	if err != nil { return n,err }

	n.node1,err =  parseSelect(q)
	if err != nil { return n,err }
	n.node2, err = parseFrom(q)
	if err != nil { return n,err }
	n.node3,err =  parseWhere(q)
	if err != nil { return n,err }
	n.node4,err =  parseGroupby(q)
	if err != nil { return n,err }
	n.node5,err =  parseHaving(q)
	if err != nil { return n,err }
	q.sortExpr,err = parseOrder(q)
	if err != nil { return n,err }

	if q.Tok().id != EOS { err = errors.New("Expected end of query, got "+q.Tok().val) }

	//add 'having' and 'order by' expressions to selections if grouping
	if q.sortExpr!=nil && q.groupby {
		nn := n.node1.node1
		for ; nn.node2 != nil; nn = nn.node2 {}
		nn.node2 = &Node{
			label: N_SELECTIONS,
			tok3: 1<<4,
			node1: q.sortExpr,
		}
	}
	findHavingAggregates(q, n, n.node5)

	//process leaf nodes that need file data
	err = leafNodeFiles(q,n)
	treePrint(n, 0)
	if err != nil { return n,err }

	//process selections
	_,_,_,err = aggCheck(n.node1)
	if err != nil { return n,err }
	_,_,_,err = typeCheck(n.node1)
	if err != nil { return n,err }
	branchShortener(q, n.node1)
	columnNamer(q, n.node1)
	_,f := findAggregateFunctions(q, n.node1)
	if f { return n,errors.New("Cannot have aggregate function inside an aggregate function") }

	//process 'where' section
	if e := findAggregateFunction(n.node3);e >0 { return n,errors.New("Cannot have aggregate function in 'where' clause") }
	_,_,_,err = typeCheck(n.node3)
	if err != nil { return n,err }
	branchShortener(q, n.node3.node1)

	//process groups
	_,_,_,err = typeCheck(n.node4)
	if err != nil { return n,err }
	branchShortener(q, n.node4)

	//process sort expression separately if not grouping
	if !(q.sortExpr!=nil && q.groupby) {
		_,sortType,_,er := typeCheck(q.sortExpr)
		if er != nil { return n,er }
		err = enforceType(q.sortExpr, sortType)
		branchShortener(q,q.sortExpr)
	}

	return n,err
}

//node1 is selections
func parseSelect(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECT}
	var err error
	if q.Tok().val == "c" { q.intColumn = true; q.NextTok() }
	if q.Tok().id != KW_SELECT { return n,errors.New("Expected query to start with 'select'. Found "+q.Tok().val) }
	q.NextTok()
	err = parseTop(q)
	if err != nil { return n,err }
	countSelected = 0
	n.node1,err = parseSelections(q)
	return n,err
}

//node2 is chain of selections for all infile columns
func selectAll(q* QuerySpecs) (*Node,error) {
	var err error
	n := &Node{label:N_SELECTIONS,tok3:0}
	file := q.files["_f1"]
	firstSelection := n
	var lastSelection *Node
	for i:= range file.names {
		n.node2 = &Node{label:N_SELECTIONS,tok3:0}
		n.node1 = &Node{
			label: N_VALUE,
			tok1: i,
			tok2: 1,
		}
		countSelected++
		lastSelection = n
		n = n.node2
	}
	lastSelection.node2,err = parseSelections(q)
	return firstSelection,err
}

//node1 is expression
//node2 is next selection
//tok1 is destination column indexes
//tok2 will be destination column name
//tok3 is bit array - 1 and 2 are distinct
//tok4 will be aggregate function
//tok5 will be type
func parseSelections(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECTIONS}
	var err error
	var hidden bool
	if q.Tok().id == SP_COMMA { q.NextTok() }
	n.tok3 = 0
	switch q.Tok().id {
	case SP_STAR:
		q.NextTok()
		return selectAll(q)
	//tok3 bit 1 means distinct, bit 2 means hidden
	case KW_DISTINCT:
		n.tok3 = 1
		q.NextTok()
		if q.Tok().val == "hidden" && !q.Tok().quoted { hidden = true; n.tok3=3; q.NextTok() }
		fallthrough
	//expression
	case KW_CASE:     fallthrough
	case WORD:        fallthrough
	case SP_MINUS:    fallthrough
	case SP_LPAREN:
		if !hidden { countSelected++ }
		//alias = expression
		if q.PeekTok().id == SP_EQ {
			if q.Tok().id != WORD { return n,errors.New("Alias must be a word. Found "+q.Tok().val) }
			n.tok2 = q.Tok().val
			q.NextTok()
			q.NextTok()
			n.node1,err = parseExprAdd(q)
		//expression
		} else {
			n.node1,err = parseExprAdd(q)
			if q.Tok().id == KW_AS {
				n.tok2 = q.NextTok().val
				q.NextTok()
			}
		}
		if err != nil { return n,err }
		n.node2,err = parseSelections(q)
		return n,err
	//done with selections
	case KW_FROM:
		if countSelected == 0 { return selectAll(q) }
		return nil,nil
	default: return n,errors.New("Expected a new selection or 'from' clause. Found "+q.Tok().val)
	}
	return n,err
}

//node1 is exprMult
//node2 is exprAdd
//tok1 is add/minus operator
func parseExprAdd(q* QuerySpecs) (*Node,error) {
	var err error
	n := &Node{label:N_EXPRADD}
	n.node1,err = parseExprMult(q)
	if err != nil { return n,err }
	switch q.Tok().id {
	case SP_MINUS: fallthrough
	case SP_PLUS:
		n.tok1 = q.Tok().id
		q.NextTok()
		n.node2,err = parseExprAdd(q)
	}
	return n, err
}

//node1 is exprNeg
//node2 is exprMult
//tok1 is mult/div operator
func parseExprMult(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_EXPRMULT}
	var err error
	n.node1,err = parseExprNeg(q)
	if err != nil { return n,err }
	switch q.Tok().id {
	case SP_STAR: fallthrough
	case SP_MOD: fallthrough
	case SP_CARROT: fallthrough
	case SP_DIV:
		if q.PeekTok().id == KW_FROM { break }
		n.tok1 = q.Tok().id
		q.NextTok()
		n.node2,err = parseExprMult(q)
	}
	return n, err
}

//tok1 is minus operator
//node1 is exprCase
func parseExprNeg(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_EXPRNEG}
	var err error
	if q.Tok().id == SP_MINUS {
		n.tok1 = q.Tok().id
		q.NextTok()
	}
	n.node1, err = parseExprCase(q)
	return n, err
}

//tok1 is [case, word, expr] token - tells if case, terminal value, or (expr)
//tok2 is [when, expr] token - tells what kind of case. predlist, or expr exprlist respectively
//node2.tok3 will be initial 'when' expression type
//node1 is (expression), when predicate list, expression for exprlist
//node2 is expression list to compare to initial expression
//node3 is else expression
func parseExprCase(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_EXPRCASE}
	var err error
	switch q.Tok().id {
	case KW_CASE:
		n.tok1 = KW_CASE

		switch q.NextTok().id {
		//when predicates are true
		case KW_WHEN:
			n.tok2 = KW_WHEN
			n.node1,err = parseCaseWhenPredList(q)
			if err != nil { return n,err }
		//expression matches expression list
		case WORD: fallthrough
		case SP_LPAREN:
			db.Print2("case starts with expression:", q.Tok())
			n.tok2 = N_EXPRADD
			n.node1,err = parseExprAdd(q)
			if err != nil { return n,err }
			if q.Tok().id != KW_WHEN { return n,errors.New("Expected 'when' after case expression. Found "+q.Tok().val) }
			n.node2,err = parseCaseWhenExprList(q)
			if err != nil { return n,err }
		default: return n,errors.New("Expected expression or 'when'. Found "+q.Tok().val)
		}

		switch q.Tok().id {
		case KW_END:
			q.NextTok()
		case KW_ELSE:
			q.NextTok()
			n.node3,err = parseExprAdd(q)
			if err != nil { return n,err }
			if q.Tok().id != KW_END { return n,errors.New("Expected 'end' after 'else' expression. Found "+q.Tok().val) }
			q.NextTok()
		default:
			return n,errors.New("Expected 'end' or 'else' after case expression. Found "+q.Tok().val)
		}

	case WORD:
		n.tok1 = WORD
		n.node1,err = parseValue(q)
	case SP_LPAREN:
		n.tok1 = N_EXPRADD
		q.NextTok()
		n.node1,err = parseExprAdd(q)
		if err != nil { return n,err }
		if q.Tok().id != SP_RPAREN { return n,errors.New("Expected closing parenthesis. Found "+q.Tok().val) }
		q.NextTok()
	default: return n,errors.New("Expected case, value, or expression. Found "+q.Tok().val)
	}
	return n, err
}

//TODO: do much of this in leafNodeFiles()
//if implement dot notation, put parser here
//tok1 is [value, column index, function id]
//tok2 is [0,1,2] for literal/column/function
//tok3 is type
//tok4 is type in special cases like FN_COUNT
//node1 is function expression if doing that
var cInt *regexp.Regexp = regexp.MustCompile(`^c\d+$`)
func parseValue(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_VALUE}
	var err error
	tok := q.Tok()
	//see if it's a function
	if fn,ok := functionMap[tok.val]; ok && !tok.quoted && q.PeekTok().id==SP_LPAREN {
		n.tok1 = fn
		n.tok2 = 2
		n.node1, err = parseFunction(q)
		if err != nil { return n,err }
		return n, err
	//any non-function value
	} else {
		//determine file source and value
		S := strings.SplitAfterN(tok.val,".",2)
		var fdata *FileData
		var ok bool
		var value string
		if len(S)==2 && q.aliases != nil {
			alias := strings.TrimRight(S[0],".")
			fdata,ok = q.files[alias]
			value = S[1]
			if !ok { value = tok.val; fdata = q.files["_f1"] }
		} else {
			value = tok.val
			fdata = q.files["_f1"]
		}
		//try column number
		if num,er := Atoi(value); q.intColumn && !tok.quoted && er == nil {
			if num<0 || num>fdata.width { return n,errors.New("Column number out of bounds:"+Sprint(num)) }
			n.tok1 = num-1
			n.tok2 = 1
			n.tok3 = fdata.types[num-1]
		} else if !tok.quoted && cInt.MatchString(value) {
			num,_ := Atoi(value[1:])
			if num<0 || num>fdata.width { return n,errors.New("Column number out of bounds:"+Sprint(num)) }
			n.tok1 = num - 1
			n.tok2 = 1
			n.tok3 = fdata.types[num-1]
		//try column name
		} else if n.tok1, err = getColumnIdx(fdata.names, value); err == nil {
			n.tok2 = 1
			n.tok3 = fdata.types[n.tok1.(int)]
		//else must be literal
		} else {
			err = nil
			n.tok2 = 0
			n.tok3 = getNarrowestType(value,0)
			n.tok1 = value
		}
	}
	q.NextTok()
	return n, err
}

//tok1 says if more predicates
//node1 is case predicate
//node2 is next case predicate list node
func parseCaseWhenPredList(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_CPREDLIST}
	var err error
	n.node1,err = parseCasePredicate(q)
	if err != nil { return n,err }
	if q.Tok().id == KW_WHEN {
		n.tok1 = 1
		n.node2,err = parseCaseWhenPredList(q)
	}
	return n, err
}

//node1 is predicates
//node2 is expression if true
func parseCasePredicate(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_CPRED}
	var err error
	q.NextTok() //eat when token
	n.node1,err = parsePredicates(q)
	if err != nil { return n,err }
	if q.Tok().id != KW_THEN { return n,errors.New("Expected 'then' after predicate. Found: "+q.Tok().val) }
	q.NextTok() //eat then token
	n.node2,err = parseExprAdd(q)
	return n, err
}

//tok1 is logop
//tok2 is negation
//node1 is predicate comparison
//node2 is next predicates node
func parsePredicates(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_PREDICATES}
	var err error
	n.tok2 = 0
	if q.Tok().id == SP_NEGATE { n.tok2 = 1; q.NextTok() }
	n.node1,err = parsePredCompare(q)
	if err != nil { return n,err }
	if (q.Tok().id & LOGOP) != 0 {
		n.tok1 = q.Tok().id
		q.NextTok()
		n.node2, err = parsePredicates(q)
	}
	return n, err
}

//tok1 is [relop, paren] for comparison or more predicates
//tok2 is negation
//tok3 will be independant type
//node1 is [expr, predicates]
//node2 is second expr
//node3 is third expr for betweens
func parsePredCompare(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_PREDCOMP}
	var err error
	var negate int
	var olderr error
	if q.Tok().id == SP_NEGATE { negate ^= 1; q.NextTok() }
	//more predicates in parentheses
	if q.Tok().id == SP_LPAREN {
		pos := q.tokIdx
		//try parsing as predicate
		q.NextTok()
		n.node1, err = parsePredicates(q)
		if q.Tok().id != SP_RPAREN { return n,errors.New("Expected cosing parenthesis. Found:"+q.Tok().val) }
		q.NextTok()
		//if failed, reparse as expression
		if err != nil {
			q.tokIdx = pos
			olderr = err
		} else { return n,err }
	}
	//comparison
	n.node1, err = parseExprAdd(q)
	if err != nil && olderr != nil { return n,olderr }
	if err != nil { return n,err }
	if q.Tok().id == SP_NEGATE { negate ^= 1; q.NextTok() }
	n.tok2 = negate
	if (q.Tok().id & RELOP) == 0 { return n,errors.New("Expected relational operator. Found: "+q.Tok().val) }
	n.tok1 = q.Tok().id
	q.NextTok()
	if n.tok1 == KW_LIKE {
		var like interface{}
		re := regexp.MustCompile("%")
		like = re.ReplaceAllString(q.Tok().val, ".*")
		re = regexp.MustCompile("_")
		like = re.ReplaceAllString(like.(string), ".")
		like,err = regexp.Compile("(?i)^"+like.(string)+"$")
		n.node2 = &Node{label: N_VALUE, tok1: liker{like.(*regexp.Regexp)}, tok2: 0, tok3: 0} //like gets 'null' type because it also doesn't effect operation type
		q.NextTok()
	} else {
		n.node2, err = parseExprAdd(q)
		if err != nil { return n,err }
	}
	if n.tok1 == KW_BETWEEN {
		q.NextTok()
		n.node3, err = parseExprAdd(q)
	}
	return n, err
}

//tok1 int tells that there's another
//node1 is case expression
//node2 is next exprlist node
func parseCaseWhenExprList(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_CWEXPRLIST}
	var err error
	n.node1, err = parseCaseWhenExpr(q)
	if err != nil { return n,err }
	if q.Tok().id == KW_WHEN {
		n.tok1 = 1
		n.node2, err = parseCaseWhenExprList(q)
	}
	return n, err
}

//node1 is comparison expression
//node2 is result expression
func parseCaseWhenExpr(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_CWEXPR}
	var err error
	q.NextTok() //eat when token
	n.node1,err = parseExprAdd(q)
	if err != nil { return n,err }
	q.NextTok() //eat then token
	n.node2,err = parseExprAdd(q)
	return n, err
}

//row limit
func parseTop(q* QuerySpecs) error {
	var err error
	if q.Tok().id == KW_TOP {
		q.quantityLimit, err = Atoi(q.PeekTok().val)
		if err != nil { return errors.New("Expected number after 'top'. Found "+q.PeekTok().val) }
		q.NextTok(); q.NextTok()
	}
	return nil
}

//tok1 is file path
//tok2 is alias
//node1 is joins
func parseFrom(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_FROM}
	var err error
	if q.Tok().id != KW_FROM { return n,errors.New("Expected 'from'. Found: "+q.Tok().val) }
	n.tok1 = q.NextTok()
	q.NextTok()
	t := q.Tok()
	switch t.id {
	case KW_AS:
		t = q.NextTok()
		if t.id != WORD { return n,errors.New("Expected alias after as. Found: "+t.val) }
		fallthrough
	case WORD:
		if _,ok:=joinMap[t.val];ok { return n,errors.New("Join requires file aliases. Found: "+t.val) }
		n.tok2 = t.val
		q.NextTok()
	}
	n.node1, err = parseJoinChain(q)
	return n, err
}

//node1 is join
//node2 is next joinChain
func parseJoinChain(q *QuerySpecs) (*Node,error) {
	n := &Node{label:N_JOINCHAIN}
	var err error
	switch q.Tok().Lower() {
	case "left":
	case "right":
	case "inner":
	case "outer":
	case "join":
	default: return nil,nil
	}
	n.node1, err = parseJoin(q)
	if err != nil { return n,err }
	n.node2, err = parseJoinChain(q)
	return n, err
}
//tok1 is [left right]
//tok2 is [inner outer]
//tok3 is filepath
//tok4 is alias
//node1 is join condition (predicates)
func parseJoin(q *QuerySpecs) (*Node,error) {
	n := &Node{label:N_JOIN}
	var err error
	switch q.Tok().Lower() {
	case "left":  n.tok1 = KW_LEFT; q.NextTok();
	case "right": n.tok1 = KW_RIGHT; q.NextTok();
	}
	switch q.Tok().Lower() {
	case "inner": n.tok2 = KW_INNER; q.NextTok();
	case "outer": n.tok2 = KW_OUTER; q.NextTok();
	}
	if q.Tok().Lower() != "join" { return n,errors.New("Expected 'join'. Found:"+q.Tok().val) }
	//file path
	n.tok3 = q.NextTok().val
	if err:=eosError(q);err != nil { return n,err }
	//alias
	t := q.NextTok()
	switch t.id {
	case KW_AS:
		t = q.NextTok()
		if t.id != WORD { return n,errors.New("Expected alias after as. Found: "+t.val) }
		fallthrough
	case WORD:
		n.tok4 = t.val
	default:
		return n,errors.New("Join requires an alias. Found: "+q.Tok().val)
	}
	if _,ok:=q.files[t.val];!ok { return n,errors.New("Could not open file "+n.tok3.(string)) }
	if q.NextTok().Lower() != "on" { return n,errors.New("Expected 'on'. Found: "+q.Tok().val) }
	q.NextTok()
	n.node1, err = parsePredicates(q)
	return n, err
}

//node1 is conditions
func parseWhere(q*QuerySpecs) (*Node,error) {
	n := &Node{label:N_WHERE}
	var err error
	if q.Tok().id != KW_WHERE { return n,nil }
	q.NextTok()
	n.node1,err = parsePredicates(q)
	return n,err
}

//node1 is conditions
func parseHaving(q*QuerySpecs) (*Node,error) {
	n := &Node{label:N_WHERE}
	var err error
	if q.Tok().id != KW_HAVING { return n,nil }
	q.NextTok()
	n.node1,err = parsePredicates(q)
	return n,err
}

//doesn't add to parse tree yet, juset sets q member
func parseOrder(q* QuerySpecs) (*Node,error) {
	if q.Tok().id == EOS { return nil,nil }
	if q.Tok().id == KW_ORDER {
		if q.NextTok().id != KW_BY { return nil,errors.New("Expected 'by' after 'order'. Found "+q.Tok().val) }
		q.NextTok()
		expr, err := parseExprAdd(q)
		if q.Tok().id == KW_ORDHOW { q.NextTok(); q.sortWay = 2 }
		return expr, err
	}
	return nil,nil
}


//tok1 is function id
//tok2 will be intermediate index if aggragate
//tok3 is distinct btree for count
//node1 is expression in parens
func parseFunction(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_FUNCTION}
	var err error
	n.tok1 = functionMap[q.Tok().val]
	q.NextTok()
	//count(*)
	if q.NextTok().id == SP_STAR && n.tok1.(int) == FN_COUNT {
		n.node1 = &Node{
			label: N_VALUE,
			tok1: "1",
			tok2: 0,
			tok3: 1,
		}
		q.NextTok()
	//other functions
	} else {
		if q.Tok().val == "distinct" {
			n.tok3 = bt.New(200)
			q.NextTok()
		}
		n.node1, err = parseExprAdd(q)
	}
	if q.Tok().id != SP_RPAREN { return n,errors.New("Expected closing parenthesis after function. Found: "+q.Tok().val) }
	q.NextTok()
	//groupby if aggregate function
	if (n.tok1.(int) & AGG_BIT) != 0 { q.groupby = true }
	return n,err
}

//node1 is groupExpressions
//tok1 is groups map
func parseGroupby(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_GROUPBY}
	var err error
	if !(q.Tok().val == "group" && q.PeekTok().val == "by") { return nil,nil }
	q.NextTok()
	q.NextTok()
	n.node1, err = parseGroupExpressions(q)
	n.tok1 = make(map[interface{}]interface{})
	return n,err
}

//node1 is expression
//node2 is expressions
//tok1 [0,1] for map returns row or next map
func parseGroupExpressions(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_EXPRESSIONS}
	var err error
	n.node1, err = parseExprAdd(q)
	if err != nil { return n,err }
	switch q.Tok().id {
		case SP_COMMA: q.NextTok()
		case WORD:
		case KW_CASE:
		case SP_LPAREN:
		default:
			n.tok1 = 0
			return n,err
	}
	n.tok1 = 1
	n.node2, err = parseGroupExpressions(q)
	return n,err
}
