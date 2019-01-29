import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {getUnique,getWhere,colIndex} from './utils.js';
import * as serviceWorker from './serviceWorker';

var testserver = true;

//drop down list for choosing section of metadata table
class TableList extends React.Component {
    tableBut(name,idx){
        return (
            <option className="tableButton1" key={idx} onClick={()=>{ this.props.changeTable(name) }}>
                {name === "*" ? "All Tables" : name}
            </option>
        )
    }
    render(){
        return (
            <div className="dropmenu">
                <div className="dropButton">
                {this.props.section === "*"?"All Tables":this.props.section+"\u25bc"}
                </div>
                <select size="10" className="dropmenu-content">
                    {this.props.tableNames.concat(["*"]).map((name,i)=>{return this.tableBut(name,i)})}
                </select>
            </div>
        )
    }
}

//display html table of sql query
class TableGrid extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            hideColumns : this.props.hideColumns || [],
            childId : Math.random(),
            parentId : Math.random()
        }
    }
    hideColumn(column,numeric){
        if (!numeric)
            column = colIndex(this.props.table,column)|0;
        if (this.state.hideColumns.indexOf(column)<0){
            this.state.hideColumns.push(column);
            this.forceUpdate();
        }
    }
    row(row,type,idx){
        return( 
            <tr key={idx} className="tableRow"> 
                {row.map((name,idx)=>{ 
                    if (this.state.hideColumns.indexOf(idx)<0)
                        if (type === 'head')
                            return( <th key={idx} className="tableCell" onClick={()=>{this.hideColumn(idx,true)}}> {name} </th>)
                        else
                            return( <td key={idx} className="tableCell"> {name} </td>) })}
            </tr>
        )
    }
    render(){
        return(
            <div className="tableDiv" id={this.state.parentId}> 
            <table className="table" id={this.state.childId}>
                <tbody>
                {this.row(this.props.table.Colnames,'head')}
                {this.props.table.Vals.map((row,i)=>{return this.row(row,'entry',i)})}
                </tbody>
            </table>
            </div>
        )
    }
    resize(){
        var inner = document.getElementById(this.state.childId);
        var outter = document.getElementById(this.state.parentId);
        outter.style.maxWidth = `${inner.offsetWidth+20}px`;
    }
    componentDidUpdate(){ this.resize(); }
    componentDidMount(){ this.resize(); }
}

//query results section
class QueryRender extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            section : this.props.section || "*",
            table : getWhere(this.props.table,"TABLE_NAME",(this.props.section || "*")),
            hideColumns : this.props.hideColumns || [],
        }
    }
    changeTable(section){
        this.setState({section : section,
                       table : getWhere(this.props.table,"TABLE_NAME",section) });
    }
    render(){
        return ( 
        <div className="viewContainer">
            <TableList 
                changeTable = {(section)=>{this.changeTable(section)}}
                section = {this.state.section}
                tableNames = {getUnique(this.props.table,"TABLE_NAME")}
            />
            <div className="filler"></div>
            <TableGrid
                table = {this.state.table}
                hideColumns = {this.state.hideColumns}
            />
        </div>
        )
    }
}

class Main extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            metaTable : this.props.schemaData[0],
            metaTableNames : this.props.tableNames
        }
    }

    render(){
        return (
            <>
            <QueryRender 
                table = {this.props.schemaData[0]}
            />
            <QueryRender 
                table = {this.props.schemaData[1]}
            />
            <QueryRender 
                table = {this.props.schemaData[2]}
            />
            <QueryRender 
                table = {this.props.schemaData[3]}
            />
            </>
        )
    }
}

function startRender(initialData){
    ReactDOM.render(
        <Main 
            schemaData = {initialData}
            tableNames = {getUnique(initialData[0],"TABLE_NAME")}
        />, 
        document.getElementById('root'));
}
//running on the go server
if (! testserver){
    fetch('/query')
    .then((resp) => resp.json())
    .then(function(data) {
        console.log('first async fetch func');
        console.log(JSON.stringify(data));
        startRender(data);
      })
}
//running on the react test server
else {
    var data = require('./schema.json');
    startRender(data);
}



// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
