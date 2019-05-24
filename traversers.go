package main
import (
  . "fmt"
  s "strconv"
  "regexp"
  "time"
  "errors"
)

//traverse where branch of parse tree
func execWhere(q *QuerySpecs, fromRow *[]interface{}) (bool, error) {
    node := q.Tree.node3
    if node.node1 == nil { return true,nil }
    return wTraverse(q, node.node1, fromRow)
}

//main where section traverser
func wTraverse(q *QuerySpecs, n *Node, r *[]interface{}) (bool, error) {
    if n == nil { return false,nil }

    switch n.label {
        case N_CONDITIONS:
            match, err := wTraverse(q,n.node1,r)
            match2, err := wTraverse(q,n.node2,r)
            switch q.tempVal.(int) {
                case KW_AND:
                    match = match && match2
                case KW_OR:
                    match = match || match2
            }
            if n.tok1 == SP_NEGATE { match = !match }
            return match, err

        case N_COMPARE:
            return execRelop(n.tok1.(treeTok), n.node1, r)

        case N_MORE:
            if n.tok1 == nil { q.tempVal = 0; return true,nil }
            match2,err := wTraverse(q,n.node1,r)
            q.tempVal = n.tok1
            return match2, err

        default:
            wTraverse(q,n.node1,r)
            wTraverse(q,n.node2,r)
            wTraverse(q,n.node3,r)
            wTraverse(q,n.node4,r)
    }
    println("returning false"+s.Itoa(n.label))
    return false,nil
}

//each comparison
func execRelop(c treeTok, n *Node, r *[]interface{}) (bool, error) {
    match := false
    colVal := (*r)[c.Val.(int)]
    relop := n.tok2.(treeTok)
    compVal := n.tok3.(treeTok)
    negate := 0
    if n.tok1 == SP_NEGATE { negate ^= 1 }

    //if neither comparison value or column are null
    if compVal.Val != nil && colVal != nil {
        switch relop.Id {
            case KW_LIKE:  match = compVal.Val.(*regexp.Regexp).MatchString(Sprint(colVal))
            case SP_NOEQ: negate ^= 1
                       fallthrough
            case SP_EQ :
                switch compVal.Dtype {
                    case T_DATE:   match = compVal.Val.(time.Time).Equal(colVal.(time.Time))
                    default:       match = compVal.Val == colVal
                }
            case SP_LESSEQ: negate ^= 1
                       fallthrough
            case SP_GREAT :
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = colVal.(string) > compVal.Val.(string)
                    case T_INT:    match = colVal.(int) > compVal.Val.(int)
                    case T_FLOAT:  match = colVal.(float64) > compVal.Val.(float64)
                    case T_DATE:   match = colVal.(time.Time).After(compVal.Val.(time.Time))
                }
            case SP_GREATEQ : negate ^= 1
                       fallthrough
            case SP_LESS:
                switch compVal.Dtype {
                    case T_NULL:   fallthrough
                    case T_STRING: match = colVal.(string) < compVal.Val.(string)
                    case T_INT:    match = colVal.(int) < compVal.Val.(int)
                    case T_FLOAT:  match = colVal.(float64) < compVal.Val.(float64)
                    case T_DATE:   match = colVal.(time.Time).Before(compVal.Val.(time.Time))
                }
        }

    //if comparison value is null
    } else if compVal.Val == nil {
        switch relop.Id {
            case SP_NOEQ: negate ^= 1
                       fallthrough
            case SP_EQ : match = colVal == nil
            default  : return false, errors.New("Invalid operation with null: "+relop.Val.(string)+". Valid operators: = != <>")
        }
    //if only column is null
    } else if compVal.Val != nil && colVal == nil  {
        switch relop.Id {
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
    if q.SelectAll  {
        if !q.MemFull && ( q.NeedAllRows || q.QuantityRetrieved <= q.showLimit ) {
            res.Vals = append(res.Vals, *fromRow)
            q.QuantityRetrieved++
        }
        if q.Save { saver <- saveData{Type : CH_ROW, Row : fromRow} ; <-savedLine }
        return
    //otherwise retrieve the selected columns
    } else {
        selected := make([]interface{}, q.ColSpec.NewWidth)
        execSelections(q,q.Tree.node1.node1,res,fromRow,&selected,0)
    }
}
//selections branch of select node
func execSelections(q *QuerySpecs, n *Node, res*SingleQueryResult, fromRow *[]interface{}, selected *[]interface{}, count int) {
    if n.tok1 == nil {
        if !q.MemFull && ( q.NeedAllRows || q.QuantityRetrieved <= q.showLimit ) {
            res.Vals = append(res.Vals, *selected)
            q.QuantityRetrieved++
        }
        if q.Save { saver <- saveData{Type : CH_ROW, Row : selected} ; <-savedLine}
        return
    } else {
        (*selected)[count] = (*fromRow)[n.tok1.(treeTok).Val.(int)]
    }
    execSelections(q,n.node1,res,fromRow,selected,count+1)
}

//print parse tree for debuggging
func treePrint(n *Node, i int){
    if n==nil {return}
    for j:=0;j<i;j++ { Print("  ") }
    Println(enumMap[n.label+1000])
    treePrint(n.node1,i+1)
    treePrint(n.node2,i+1)
    treePrint(n.node3,i+1)
    treePrint(n.node4,i+1)
}

