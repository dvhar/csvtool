//new expression parsing - under construction
package main
/*
<Selections>        -> * <Selections> | <columnItem> <Selections> | ε
<columnItem>        -> <exprAdd> | <exprAdd> as <alias> | <alias> = <exprAdd>
<exprAdd>           -> <exprMult> + <exprAdd> | <exprMult> - <exprAdd> | <exprMult>
<exprMult>          -> <exprNeg> * <exprMult> | <exprNeg> / <exprMult> | <exprNeg>
<exprNeg>           -> - <exprCase> | <exprCase>
<exprCase           -> case <caseWhenExprList> end
                     | case <caseWhenExprList> else <exprAdd> end
                     | case <exprAdd> <casePredicateList> end
                     | case <exprAdd> <casePredicateList> else <exprAdd> end
                     | <value>
<value>             -> column | literal | ( <exprAdd> )
<caseWhenExprList>  -> <caseWhenExpr> <caseWhenExprList> | <caseWhenExpr>
<caseWhenExpr>      -> when <exprAdd> then <exprAdd>
<casePredicateList> -> <casePredicate> <casePredicateList> | <casePredicate>
<casePredicate>     -> when <predicates> then <exprAdd>
<predicates>        -> <predicateCompare> <logop> <predicates> | <predicateCompare>
<predicateCompare>  -> <exprAdd> <relop> <exprAdd> | ( predicates )
*/

//node1 is expression
//node2 is next selection
//tok1 is selection number
//TODO: handle quoted tokens in Tok() methods
func parseSelections2(q* QuerySpecs) (*Node,error) {
	n := &Node{label:N_SELECTIONS}
	var err error
	switch q.Tok().Id {
	case SP_STAR:
		selectAll(q)
		q.NextTok()
		return parseSelections2(q)
	//expression
	case KW_DISTINCT: fallthrough
	case KW_CASE:     fallthrough
	case WORD:        fallthrough
	case SP_LPAREN:
		n.tok1 = countSelected
		countSelected++
		n.node1,err = parseColumnItem(q)
		if err != nil { return n,err }
		n.node2,err = parseSelections2(q)
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
	//alias = expression
	if q.PeekTok().Id == SP_EQ {
		n.tok1 = q.Tok().Val
		n.tok2 = KW_AS
		q.NextTok()
		n.node1,err = parseExprAdd(q)
	//expression
	} else {
		if q.Tok().Id == KW_DISTINCT { n.tok3 = KW_DISTINCT; q.NextTok() }
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

//tok1 is [case, expression, literal/column] token
//tok2 is [when, expr] token
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
			q.NextTok()
			n.node1,err = parseCaseWhenExprList(q)
		//expression matches predicates
		case WORD:        fallthrough
		case SP_LPAREN:
			n.node1,err = parseExprAdd(q)
			if q.Tok().Id != KW_WHEN { return n,errors.New("Expected 'when' after case expression. Found "+q.Tok().Val) }
			q.NextTok()
			n.node2,err = parseCasePredicateList(q)
			switch q.Tok().Id {
			case KW_END:
				q.NextTok()
			case KW_ELSE:
				q.NextTok()
				n.node3,err = parseExprAdd(q)
			default:
				return n,errors.New("Expected 'end' or 'else' after case. Found "+q.Tok().Val)
			}
		}
	//TODO: determine value vs column
	case WORD:
		n.tok1 = q.Tok()
	case SP_LPAREN:
		q.NextTok()
		n.node1,err = parseExprAdd(q)
		if q.Tok().Id != SP_RPAREN { return n,errors.New("Expected closing parenthesis. Found "+q.Tok().Val) }
		q.NextTok()
	}
	return n, err
}
