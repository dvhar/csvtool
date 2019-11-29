import React from 'react';
import {getUnique,max,getWhere,sortQuery,t} from './utils.js';

//drop down list for what columns to hide
class TableSelectColumns extends React.Component {
	constructor(props){
		super(props);
		this.state = {
			title: this.props.title,
		}
	}
	itemInList(choice,idx,order){
		if (choice !== null)
		return (
			<option className={`tableButton1${this.props.hideColumns[idx]?" hiddenColumn":""}`} key={idx} onClick={()=>this.props.toggleColumn(idx)}>
				{choice}
			</option>
		)
	}

	SelectColumnDropdown(title, size, contents){
		return(
			<div className="dropmenu tableModDiv">
				<div className="dropButton tableModButton">
					{title}
				</div>
				<div className="dropmenu-content absolute-pos tableModDrop">
				<select size={String(size)} className="dropSelect">
					{contents}
				</select>
				</div>
			</div>
		)
	}

	render(){
		return (
			<div className="dropmenu tableModDiv">
				<div className="dropButton tableModButton">
					{this.props.title}
				</div>
				<div className="dropmenu-content absolute-pos tableModDrop">
				<select size={String(Math.min(20,this.props.table.Colnames.length))} className="dropSelect">
					{this.props.table.Colnames.map((name,i)=>this.itemInList(name,i))}
				</select>
				</div>
			</div>
		)
	}
}

//drop down list for viewing rows that have a certain value
//terrible code. rewrite it.
class TableSelectRows extends React.Component {
	constructor(props){
		super(props);
		this.state = {
			title: this.props.title,
			secondDrop : false,
			firstChoice : "",
			secondDropItems : [],
		}
	}
	dropItem(choice,idx,order){
		if (choice !== null)
		return (
			<option className="tableButton1" key={idx} onClick={()=>{ 
					switch (order){
						case 'first':
							this.setState({secondDrop:true,
										   firstChoice: choice,
										   secondDropItems: getUnique(this.props.table,choice) }); 
							break;
						case 'second':
							this.props.dropAction(this.state.firstChoice,choice);
							break;
					}
				}}>
				{choice}
			</option>
		)
	}
	render(){
		var dropdowns = [
				<select className="dropSelect" size={Math.min(20,this.props.firstDropItems.length)}>
					{this.props.firstDropItems.map((name,i)=>this.dropItem(name,i,'first'))}
				</select>
		];
		if (this.state.secondDrop)
			dropdowns.push(
				<select className="dropSelect" size={Math.min(20,this.state.secondDropItems.length+1)}>
					{["*"].concat(this.state.secondDropItems.sort()).map((name,i)=>this.dropItem(name,i,'second'))}
				</select>
			);
		return (
			<div className="dropmenu tableModDiv">
				<div className="dropButton tableModButton">
				{this.props.title}
				</div>
				<div className="dropmenu-content absolute-pos tableModDrop">
				{dropdowns}
				</div>
			</div>
		)
	}
}

//display html table with single query result
class TableGrid extends React.Component {
	constructor(props){
		super(props);
		this.state = {
			tableBodyId : Math.random(),
			tableBodyDivId : Math.random(),
			tableHeadId : Math.random(),
			tableHeadDivId : Math.random(),
			sortWay : 1
		}
	}
	sorter(ii){
		sortQuery(this.props.table,ii,this.state.sortWay);this.setState({sortWay:this.state.sortWay*-1});
	}
	header(){
		var names = this.props.table.Colnames.map((name,ii)=>{
			if (this.props.hideColumns[ii]===0) return (
			<th key={ii} className="tableCell" onClick={()=>this.sorter(ii)}>
				{this.props.table.Colnames[ii]}
			</th>
		)});
		var info = this.props.table.Types.map((name,ii)=>{
			if (this.props.hideColumns[ii]===0) return (
			<td key={ii} className="tableCell" onClick={()=>this.sorter(ii)}>
				{`${this.props.table.Pos[ii]} `}
				<span className="noselect">
				- {t[this.props.table.Types[ii]]}
				</span>
			</td>
		)});
		info.push( <td id="scrollBuffer">____</td> );
		return[<tr className="tableRow">{names}</tr>,<tr className="tableRow" id="typeHeader">{info}</tr>]
	}
	row(row,idx){
		return( 
			<tr key={idx} className="tableRow"> 
				{row.map((name,idx)=>{ 
					if (this.props.hideColumns[idx]===0) return( <td key={idx} className="tableCell"> {name} </td>) })}
			</tr>
		)
	}
	render(){
		if (this.props.table.Vals === null)
			this.props.table.Vals = [];
		return(
		<>
			<div className="tableDiv tableHeadDiv" id={this.state.tableHeadDivId}> 
			<table className="tableHead">
				<thead id={this.state.tableHeadId}>
				{this.header()}
				</thead>
			</table>
			</div>
			<div className="tableDiv tableBodyDiv" id={this.state.tableBodyDivId}> 
			<table className="tableBody" id={this.state.tableBodyId}>
				<tbody>
				{this.props.table.Vals.map((row,i)=>{return this.row(row,i)})}
				</tbody>
			</table>
			</div>
		</>
		)
	}
	resize(){
		var tableBodyDom	= document.getElementById(this.state.tableBodyId);
		var tableBodyDivDom = document.getElementById(this.state.tableBodyDivId);
		var tableHeadDom	= document.getElementById(this.state.tableHeadId);
		var tableHeadDivDom = document.getElementById(this.state.tableHeadDivId);
		var windoww = window.innerWidth;

		//get header table and body table cells to line up
		var tbody = tableBodyDom.childNodes[0];
		if (tbody.childNodes.length > 0 && tbody.childNodes[0].childNodes.length > 0){
			var trow = tbody.childNodes[0].childNodes;
			var bcell, hcell;
			var newWidth;
			for (var i in trow){
				bcell = trow[i];
				hcell = tableHeadDom.childNodes[0].childNodes[i];
				if (bcell.offsetWidth && hcell.offsetWidth){
					bcell.style.minWidth = hcell.style.minWidth = `0px`;
					newWidth = max(bcell.offsetWidth, hcell.offsetWidth);
						bcell.style.minWidth = hcell.style.minWidth = `${newWidth+1}px`;
				}
			}
		}

		//give header table and body the right size
		tableHeadDivDom.style.margin = 'auto';
		tableHeadDivDom.style.maxWidth =  tableBodyDivDom.style.maxWidth = `${Math.min(tableBodyDom.offsetWidth+15,windoww*1.00)}px`;
		if (tableBodyDom.offsetHeight <= tableBodyDivDom.offsetHeight)
			tableHeadDivDom.style.maxWidth =  tableBodyDivDom.style.maxWidth = `${Math.min(tableBodyDom.offsetWidth,windoww*1.00)}px`;
		//make head and body scroll together
		tableBodyDivDom.addEventListener('scroll',function(){ tableHeadDivDom.scrollLeft = tableBodyDivDom.scrollLeft; });

	}
	componentDidUpdate(){ this.resize(); }
	componentDidMount(){ this.resize(); }
}

//query results section
export class QueryRender extends React.Component {
	toggleColumn(column){
		this.props.hideColumns[column] ^= 1;
		this.forceUpdate();
	}
	render(){
		return ( 
		<div className="viewContainer">
			<div className="tableModifiers">
				<div className="tableQuery"><div> {this.props.table.Query} </div>
				<TableSelectRows 
					title = {"Show with column value\u25bc"}
					dropAction = {(column,value)=>{this.props.rows.col=column;this.props.rows.val=value;this.forceUpdate();}}
					table = {this.props.table}
					firstDropItems = {this.props.table.Colnames}
				/>
				<TableSelectColumns
					title = {"Show/Hide columns\u25bc"}
					table = {this.props.table}
					hideColumns = {this.props.hideColumns}
					toggleColumn = {(i)=>this.toggleColumn(i)}
				/>	
				<div className="dropmenu tableModDiv">
					<div className="dropButton tableModButton">
						<span>Rows: {this.props.table.Numrows}</span>
					</div>
				</div>
				</div>
			</div>
			<TableGrid
				table = {getWhere(this.props.table,this.props.rows.col,this.props.rows.val)}
				hideColumns = {this.props.hideColumns}
				toggleColumn = {(i)=>this.toggleColumn(i)}
			/>
		</div>
		)
	}
}

