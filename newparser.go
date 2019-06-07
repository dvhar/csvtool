//new expression parsing - under construction
/*
<Selections>        -> * <Selections> | <columnItem> <Selections> | ε
<columnItem>        -> <exprAdd> | <exprAdd> as <alias> | <alias> = <exprAdd>
<exprAdd>           -> <exprMult> + <exprAdd> | <exprMult> - <exprAdd> | <exprMult>
<exprMult>          -> <exprNeg> * <exprMult> | <exprNeg> / <exprMult> | <exprNeg>
<exprNeg>           -> - <exprCase> | <exprCase>
<exprCase           -> case <caseWhenPredList> end
                     | case <caseWhenPredList> else <exprAdd> end
                     | case <exprAdd> <caseWhenExprList> end
                     | case <exprAdd> <caseWhenExprList> else <exprAdd> end
                     | <value>
<value>             -> column | literal | ( <exprAdd> )
<caseWhenExprList>  -> <caseWhenExpr> <caseWhenExprList> | <caseWhenExpr>
<caseWhenExpr>      -> when <exprAdd> then <exprAdd>
<caseWhenPredList> -> <casePredicate> <caseWhenPredList> | <casePredicate>
<casePredicate>     -> when <predicates> then <exprAdd>
<predicates>        -> <predicateCompare> <logop> <predicates> | <predicateCompare>
<predicateCompare>  -> {not} <exprAdd> {not} <relop> <exprAdd> | {not} ( predicates )
*/

package main
import (
	"errors"
	. "fmt"
)
//node1 is selections
func parse2Select(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECT}
	var err error
	if q.Tok().Id != KW_SELECT { return n,errors.New("Expected query to start with 'select'. Found "+q.Tok().Val) }
	q.NextTok()
	err = parseTop(q)
	if err != nil { return n,err }
	countSelected = 0
	n.node1,err = parse2Selections(q)
	return n,err
}
//node1 is expression
//node2 is next selection
//tok1 is destination column index
func parse2Selections(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECTIONS}
	var err error
	switch q.Tok().Id {
	case SP_STAR:
		selectAll(q)
		q.NextTok()
		return parse2Selections(q)
	//expression
	case KW_DISTINCT: fallthrough
	case KW_CASE:     fallthrough
	case WORD:        fallthrough
	case SP_LPAREN:
		n.tok1 = countSelected
		countSelected++
		n.node1,err = parseColumnItem(q)
		if err != nil { return n,err }
		n.node2,err = parse2Selections(q)
		return n,err
	//done with selections
	case KW_FROM:
		if q.colSpec.NewWidth == 0 { selectAll(q) }
	}
	return n,err
}

//tok1 is alias
//tok2 is [as] for alias
//tok3 is distinct
//node1 is expression
func parseColumnItem(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_COLITEM}
	var err error
	if q.Tok().Id == KW_DISTINCT { n.tok3 = KW_DISTINCT; q.NextTok() }
	//alias = expression
	if q.PeekTok().Id == SP_EQ {
		n.tok1 = q.Tok().Val
		n.tok2 = KW_AS
		q.NextTok()
		n.node1,err = parseExprAdd(q)
	//expression
	} else {
		n.node1,err = parseExprAdd(q)
		if q.Tok().Id == KW_AS {
			n.tok2 = KW_AS
			n.tok1 = q.NextTok().Val
			q.NextTok()
		}
	}
	return n, err
}

//node1 is exprMult
//node2 is exprAdd
//tok1 is add/minus operator
func parseExprAdd(q* QuerySpecs) (*Node,error) {
	var err error
	n := &Node{label:N_EXPRADD}
	n.node1,err = parseExprMult(q)
	switch q.Tok().Id {
	case SP_MINUS: fallthrough
	case SP_PLUS:
		n.tok1 = q.Tok().Id
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
	switch q.Tok().Id {
	case SP_STAR: fallthrough
	case SP_DIV:
		n.tok1 = q.Tok().Id
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
	if q.Tok().Id == SP_MINUS {
		n.tok1 = q.Tok().Id
		q.NextTok()
	}
	n.node1, err = parseExprCase(q)
	return n, err
}

//tok1 is [case, word, expr] token - tells if case, terminal value, or (expr)
//tok2 is [when, expr] token - tells what kind of case. predlist, or expr exprlist respectively
//node1 is (expression), when expression list, expression for predicates
//node2 is predicate list
//node3 is else expression
func parseExprCase(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_EXPRCASE}
	var err error
	switch q.Tok().Id {
	case KW_CASE:
		n.tok1 = q.Tok()
		switch q.NextTok().Id {
		//when expressions are true
		case KW_WHEN:
			n.tok2 = q.Tok().Id
			n.node1,err = parseCaseWhenPredList(q)
		//expression matches predicates
		case WORD: fallthrough
		case SP_LPAREN:
			n.tok2 = N_EXPRADD
			n.node1,err = parseExprAdd(q)
			if q.Tok().Id != KW_WHEN { return n,errors.New("Expected 'when' after case expression. Found "+q.Tok().Val) }
			q.NextTok()
			n.node2,err = parseCaseWhenExprList(q)
			switch q.Tok().Id {
			case KW_END:
				q.NextTok()
			case KW_ELSE:
				q.NextTok()
				n.node3,err = parseExprAdd(q)
				if q.Tok().Id != KW_END { return n,errors.New("Expected 'end' after 'else' expression. Found "+q.Tok().Val) }
				q.NextTok()
			default:
				return n,errors.New("Expected 'end' or 'else' after case. Found "+q.Tok().Val)
			}
		}
	//TODO: determine value vs column
	case WORD:
		n.tok1 = q.Tok()
		q.NextTok()
	case SP_LPAREN:
		n.tok1 = N_EXPRADD
		q.NextTok()
		n.node1,err = parseExprAdd(q)
		if q.Tok().Id != SP_RPAREN { return n,errors.New("Expected closing parenthesis. Found "+q.Tok().Val) }
		q.NextTok()
	}
	return n, err
}

/*
<exprCase           -> case <caseWhenPredList> end
                     | case <caseWhenPredList> else <exprAdd> end
                     | case <exprAdd> <caseWhenExprList> end
                     | case <exprAdd> <caseWhenExprList> else <exprAdd> end
                     | <value>
<caseWhenExprList>  -> <caseWhenExpr> <caseWhenExprList> | <caseWhenExpr>
<caseWhenExpr>      -> when <exprAdd> then <exprAdd>
<caseWhenPredList> -> <casePredicate> <caseWhenPredList> | <casePredicate>
<casePredicate>     -> when <predicates> then <exprAdd>
<predicates>        -> <predicateCompare> <logop> <predicates> | <predicateCompare>
<predicateCompare>  -> {not} <exprAdd> {not} <relop> <exprAdd> | {not} ( predicates )
*/

func parseCaseWhenPredList(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_CPREDLIST}
	var err error
	if q.Tok().Id == KW_WHEN {
		q.NextTok()
	}
	return n, err
}

func parseCaseWhenExprList(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_CWEXPRLIST}
	var err error
	return n, err
}
