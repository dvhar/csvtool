import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {getUnique,getWhere,colIndex,sortQuery} from './utils.js';
import * as serviceWorker from './serviceWorker';

var testserver = true;
//var squel = require("squel");


function dropdown(title,size,contents){
    return(
        <div className="dropmenu">
            <div className="dropButton">
                {title}
            </div>
            <div className="dropmenu-content">
            <select size={String(size)} className="dropSelect">
                {contents}
            </select>
            </div>
        </div>
    )
}
//drop down list for what columns to hide
class TableSelectColumns extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            title: this.props.title,
        }
    }
    dropItem(choice,idx,order){
        if (choice !== null)
        return (
            <option className={`tableButton1${this.props.hideColumns[idx]?" hiddenColumn":""}`} key={idx} onClick={()=>this.props.toggleColumn(idx)}>
                {choice}
            </option>
        )
    }
    render(){
        return (
            dropdown(this.props.title,
                     Math.min(20,this.props.table.Colnames.length),
                     this.props.table.Colnames.map((name,i)=>this.dropItem(name,i)))
        )
    }
}

//drop down list for choosing section of table
//required props: title, table, firstDropItems, secondDropItems, dropAction
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
                    {["*"].concat(this.state.secondDropItems).map((name,i)=>this.dropItem(name,i,'second'))}
                </select>
            );
        return (
            <div className="dropmenu">
                <div className="dropButton">
                {this.props.title}
                </div>
                <div className="dropmenu-content">
                {dropdowns}
                </div>
            </div>
        )
    }
}

//display html table of sql query
//required props: hideColumns, table
class TableGrid extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            childId : Math.random(),
            parentId : Math.random()
        }
    }
    row(row,type,idx){
        return( 
            <tr key={idx} className="tableRow"> 
                {row.map((name,idx)=>{ 
                    if (this.props.hideColumns[idx]===0)
                        if (type === 'head')
                            return( <th key={idx} className="tableCell" onClick={()=>{sortQuery(this.props.table,idx);this.forceUpdate();}}> {name} </th>)
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
//required props: table
class QueryRender extends React.Component {
    toggleColumn(column){
        this.props.hideColumns[column] ^= 1;
        this.forceUpdate();
    }
    render(){
        return ( 
        <div className="viewContainer">
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
            <TableGrid
                table = {getWhere(this.props.table,this.props.rows.col,this.props.rows.val)}
                hideColumns = {this.props.hideColumns}
                toggleColumn = {(i)=>this.toggleColumn(i)}
            />
        </div>
        )
    }
}

class QuerySelect extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            showQuery : <></>,
        }
    }
    changeQuery(i){
        var tab = this.props.schemaData[i];
        this.setState({
                showQuery : <QueryRender 
                        table = {tab} 
                        hideColumns = {new Int8Array(tab.Numcols)}
                        rows = {new Object({col:"",val:"*"})}
                    />,
            });
    }
    render(){
        var menu = ( dropdown(<h2>View database schema query{"\u25bc"}</h2>,
                              String(this.props.metaTables.length),
                              this.props.metaTables.map((v,i)=> <option onClick={()=>{this.changeQuery(i)}}>{v}</option> )
                    ));
        return [menu,this.state.showQuery];
    }
}

class Main extends React.Component {
    constructor(props){
        super(props);
    }
    render(){
        return (
        <QuerySelect
            schemaData = {this.props.schemaData}
            metaTables = {this.props.metaTables}
        />
        )
    }
}

function startRender(initialData){
    ReactDOM.render(
        <Main 
            schemaData = {initialData}
            metaTables = {["column info abridged","table key info","informationschema.colums","column info with keys"]}
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
