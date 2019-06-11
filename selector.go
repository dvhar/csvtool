package main

//select node of tree root
func execSelect2(q *QuerySpecs, res*SingleQueryResult, fromRow *[]interface{}) {
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

func newColItem(q* QuerySpecs, idx, typ int, name string) {
	q.colSpec.NewNames = append(q.colSpec.NewNames, name)
	q.colSpec.NewTypes = append(q.colSpec.NewTypes, typ)
	q.colSpec.NewPos = append(q.colSpec.NewPos, idx+1)
	q.colSpec.NewWidth++
}
