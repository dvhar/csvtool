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
            return execRelop(n.tok1.(treeTok), n.node1, r)

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
        (*selected)[n.tok2.(int)] = (*fromRow)[n.tok1.(treeTok).Val.(int)]
    }
    execSelections(q,n.node1,res,fromRow,selected)
}

//print parse tree for debuggging
func treePrint(n *Node, i int){
    if n==nil {return}
    for j:=0;j<i;j++ { Print("  ") }
    Println(treeMap[n.label])
    treePrint(n.node1,i+1)
    treePrint(n.node2,i+1)
    treePrint(n.node3,i+1)
}

//tree node labels for debugging
var treeMap = map[int]string {
    N_PPTOKENS:   "N_PPTOKENS",
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
    N_ORDERM:     "N_ORDERM",
}
