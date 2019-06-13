//new expression parsing - under construction
/*
<Select>            -> {c|n} Select { <top> } <Selections>
<Selections>        -> * <Selections> | <columnItem> <Selections> | ε
<columnItem>        -> <exprAdd> | <exprAdd> as <alias> | <alias> = <exprAdd>
<exprAdd>           -> <exprMult> + <exprAdd> | <exprMult> - <exprAdd> | <exprMult>
<exprMult>          -> <exprNeg> * <exprMult> | <exprNeg> / <exprMult> | <exprNeg>
<exprNeg>           -> - <exprCase> | <exprCase>
<exprCase>          -> case <caseWhenPredList> end
                     | case <caseWhenPredList> else <exprAdd> end
                     | case <exprAdd> <caseWhenExprList> end
                     | case <exprAdd> <caseWhenExprList> else <exprAdd> end
                     | <value>
<value>             -> column | literal | ( expression )
<caseWhenExprList>  -> <caseWhenExpr> <caseWhenExprList> | <caseWhenExpr>
<caseWhenExpr>      -> when <exprAdd> then <exprAdd>
<caseWhenPredList>  -> <casePredicate> <caseWhenPredList> | <casePredicate>
<casePredicate>     -> when <predicates> then <exprAdd>
<predicates>        -> <predicateCompare> <logop> <predicates> | <predicateCompare>
<predicateCompare>  -> {not} <exprAdd> {not} <relop> <exprAdd> 
                     | {not} <exprAdd> {not} between <exprAdd> and <exprAdd>
                     | {not} ( predicates )

ints: column unless c2 present, overridden by c or n before select
*/


package main
import (
	"errors"
	"regexp"
	. "strconv"
	. "fmt"
)

//recursive descent parser builds parse tree and QuerySpecs
func parseQuery(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_QUERY}
	n.tok1 = q
	lineNo = 1
	err := scanTokens(q)
	if err != nil { return n,err }
	err = openFiles(q)
	if err != nil { return n,err }

	//new expression parser test
	n.node1,err =  parse2Select(q)
	if err != nil { return n,err }
	_,_,_,err = typeCheck(n.node1)
	if err != nil {Println("err:",err); return n,err }
	branchShortener(q, n.node1)
	columnNamer(q, n.node1)
	treePrint(n.node1,0)

	n.node2, err = parseFrom(q)
	if err != nil { return n,err }
	n.node3,err =  parseWhere(q)
	if err != nil { return n,err }
	_,_,_,err = typeCheck(n.node3)
	if err != nil {Println("err:",err); return n,err }
	branchShortener(q, n.node3.node1)
	err =  parseOrder(q)
	if err != nil { return n,err }
	if q.Tok().id != EOS { err = errors.New("Expected end of query, got "+q.Tok().val) }
	return n,err
}

//node1 is selections
func parse2Select(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECT}
	var err error
	if q.Tok().val == "c" { q.intColumn = true; q.NextTok() }
	if q.Tok().val == "n" { q.intColumn = false; q.NextTok() }
	if q.Tok().id != KW_SELECT { return n,errors.New("Expected query to start with 'select'. Found "+q.Tok().val) }
	q.NextTok()
	err = parseTop(q)
	if err != nil { return n,err }
	countSelected = 0
	n.node1,err = parse2Selections(q)
	return n,err
}

//node2 is chain of selections for all infile columns
func selectAll2(q* QuerySpecs) (*Node,error) {
	var err error
	n := &Node{label:N_SELECTIONS}
	file := q.files["_fmk01"]
	firstSelection := n
	var lastSelection *Node
	for i:= range file.names {
		n.tok2  = file.names[i]
		n.node2 = &Node{label:N_SELECTIONS}
		n.node1 = &Node{
			label: N_COLITEM,
			tok1: file.names[i],
			tok3: file.types[i],
			node1: &Node{
				label: N_VALUE,
				tok1: i,
				tok2: 1,
				tok3: file.types[i],
			},
		}
		countSelected++
		lastSelection = n
		n = n.node2
	}
	lastSelection.node2,err = parse2Selections(q)
	return firstSelection,err
}

//node1 is column item
//node2 is next selection
//tok1 is destination column index
//tok2 will be destination column name
//tok3 is external use of subtree
func parse2Selections(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECTIONS}
	var err error
	var hidden bool
	switch q.Tok().id {
	case SP_STAR:
		q.NextTok()
		return selectAll2(q)
	//expression
	case KW_DISTINCT:
		n.tok3 = 1
		q.NextTok()
		if q.Tok().val == "hidden" && !q.Tok().quoted { hidden = true; n.tok3=3; q.NextTok() }
		fallthrough
	case KW_CASE:     fallthrough
	case WORD:        fallthrough
	case SP_MINUS:        fallthrough
	case SP_LPAREN:
		Println("colitem starts with",q.Tok())
		if !hidden { countSelected++ }
		n.node1,err = parseColumnItem(q)
		if err != nil { return n,err }
		n.node2,err = parse2Selections(q)
		return n,err
	//done with selections
	case KW_FROM:
		if countSelected == 0 { return selectAll2(q) }
		return nil,nil
	}
	return n,err
}

//tok1 is alias
//tok2 is external usage of expression
//tok3 will be type
//node1 is expression
func parseColumnItem(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_COLITEM}
	var err error
	//alias = expression
	if q.PeekTok().id == SP_EQ {
		if q.Tok().id != WORD { return n,errors.New("Alias must be a word. Found "+q.Tok().val) }
		n.tok1 = q.Tok().val
		q.NextTok()
		q.NextTok()
		n.node1,err = parseExprAdd(q)
	//expression
	} else {
		n.node1,err = parseExprAdd(q)
		if q.Tok().id == KW_AS {
			n.tok1 = q.NextTok().val
			q.NextTok()
		}
	}
	return n, err
}

//node1 is exprMult
//node2 is exprAdd
//tok1 is add/minus operator
//tok3 will be type
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
//tok3 will be type
func parseExprMult(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_EXPRMULT}
	var err error
	n.node1,err = parseExprNeg(q)
	if err != nil { return n,err }
	switch q.Tok().id {
	case SP_STAR: fallthrough
	case SP_DIV:
		if q.PeekTok().id == KW_FROM { break }
		n.tok1 = q.Tok().id
		q.NextTok()
		n.node2,err = parseExprMult(q)
	}
	return n, err
}

//tok1 is minus operator
//tok3 will be type
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
//tok3 will be type
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
			Println("case starts with expression:", q.Tok())
			n.tok2 = N_EXPRADD
			n.node1,err = parseExprAdd(q)
			if err != nil { return n,err }
			if q.Tok().id != KW_WHEN { return n,errors.New("Expected 'when' after case expression. Found "+q.Tok().val) }
			n.node2,err = parseCaseWhenExprList(q)
			if err != nil { return n,err }
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
	}
	return n, err
}

//if implement dot notation, put parser here
//tok1 is [value, column index]
//tok2 is [0,1] for literal/col
//tok3 is type
func parseValue(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_VALUE}
	var err error
	cInt := regexp.MustCompile(`^c\d+$`)
	fdata := q.files["_fmk01"]
	tok := q.Tok()
	errCheck := func(col int) error {
		if col < 1 { return errors.New("Column number too small: "+Sprint(col)) }
		if col > fdata.width { return errors.New("Column number too big: "+Sprint(col)+". Max is "+Itoa(fdata.width)) }
		return nil
	}
	//given a column number
	if num,er := Atoi(tok.val); q.intColumn && !tok.quoted && er == nil {
		if err := errCheck(num); err != nil { return n,err }
		n.tok1 = num-1
		n.tok2 = 1
		n.tok3 = fdata.types[num-1]
	} else if !q.intColumn && !tok.quoted && cInt.MatchString(tok.val) {
		num,_ := Atoi(tok.val[1:])
		if err := errCheck(num); err != nil { return n,err }
		n.tok1 = num - 1
		n.tok2 = 1
		n.tok3 = fdata.types[num-1]
	//else try column name
	} else if n.tok1, err = getColumnIdx(fdata.names, tok.val); err == nil {
		n.tok2 = 1
		n.tok3 = fdata.types[n.tok1.(int)]
	//else must be literal
	} else {
		err = nil
		n.tok1 = tok.val
		n.tok2 = 0
		n.tok3 = getNarrowestType(tok.val,0)
	}
	q.NextTok()
	if n.tok2.(int)==1 { n.tok3 = fdata.types[n.tok1.(int)] }
	return n, err
}

//tok1 says if more predicates
//tok3 of case node will be type
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

//tok3 of case node will be type
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
	if q.Tok().id == SP_NEGATE { n.tok2 = SP_NEGATE; q.NextTok() }
	n.node1,err = parsePredCompare(q)
	if err != nil { return n,err }
	if (q.Tok().id & LOGOP) != 0 {
		n.tok1 = q.Tok().id
		q.NextTok()
		n.node2, err = parsePredicates(q)
	}
	return n, err
}

//modify this to immediatly compile a regular expression for 'like' relop
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
	var expression bool
	if q.Tok().id == SP_NEGATE { negate ^= 1; q.NextTok() }
	if q.Tok().id == SP_LPAREN {
		pos := q.tokIdx
		//try parsing as predicate
		q.NextTok()
		n.node1, err = parsePredicates(q)
		q.NextTok()
		//if failed, reparse as expression
		if err != nil {
			q.tokIdx = pos
			expression = true
		}
	}
	if q.Tok().id == WORD || expression {
		n.node1, err = parseExprAdd(q)
		if err != nil { return n,err }
		if q.Tok().id == SP_NEGATE { negate ^= 1; q.NextTok() }
		if negate == 1 { n.tok2 = SP_NEGATE }
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
			n.node2 = &Node{label: N_VALUE, tok1: like.(*regexp.Regexp), tok2: 0, tok3: 0} //like gets 'null' type because it also doesn't effect operation type
			q.NextTok()
		} else {
			n.node2, err = parseExprAdd(q)
			if err != nil { return n,err }
		}
		if n.tok1 == KW_BETWEEN {
			q.NextTok()
			n.node3, err = parseExprAdd(q)
		}
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
func parseFrom(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_FROM}
	if q.Tok().id != KW_FROM { return n,errors.New("Expected 'from'. Found: "+q.Tok().val) }
	n.tok1 = q.NextTok()
	q.NextTok()
	if q.Tok().id == WORD {
		n.tok2 = q.Tok()
		q.NextTok()
	}
	if q.Tok().id == KW_AS {
		n.tok2 = q.NextTok()
		q.NextTok()
	}
	return n, nil
}

//node1 is conditions
func parseWhere(q*QuerySpecs) (*Node,error) {
	n := &Node{label:N_WHERE}
	var err error
	if q.Tok().id != KW_WHERE { return n,nil }
	q.NextTok()
	//n.node1,err = parseConditions(q)
	n.node1,err = parsePredicates(q)
	return n,err
}

//currently order is only thing after where
func parseOrder(q* QuerySpecs) error {
	if q.Tok().id == EOS { return nil }
	if q.Tok().id == KW_ORDER {
		if q.NextTok().id != KW_BY { return errors.New("Expected 'by' after 'order'. Found "+q.Tok().val) }
		q.NextTok()
	}
	return nil
}
