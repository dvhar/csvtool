import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {getUnique,getWhere,colIndex} from './utils.js';
import * as serviceWorker from './serviceWorker';


class TableList extends React.Component {
    tableBut(name,idx){
        return (
            <option className="tableButton1" key={idx} onClick={()=>{ this.props.changeTable(name) }}>
                {name}
            </option>
        )
    }

    render(){
        return (
            <div className="dropmenu">
                <div className="dropButton">
                {this.props.section+"\u25bc"}
                </div>
                <select size="10" className="dropmenu-content">
                    {this.props.tableNames.map((name,i)=>{return this.tableBut(name,i)})}
                </select>
            </div>
        )
    }
}

class QueryRender extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            hideColumns : this.props.hide || [],
            table : getWhere(this.props.table,"TABLE_NAME",this.props.section),
            section : this.props.section,
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
            <div className="filler">
            </div>
            <div className="tableDiv"> 
            <table className="table">
                <tbody>
                {this.row(this.state.table.Colnames,'head')}
                {this.state.table.Vals.map((row,i)=>{return this.row(row,'entry',i)})}
                </tbody>
            </table>
            </div>
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
                table = {this.state.metaTable}
                section = {this.state.metaTableNames[0]}
            />
            <QueryRender 
                table = {this.state.metaTable}
                section = {this.state.metaTableNames[1]}
            />
            </>
        )
    }
}

fetch('/query')
.then((resp) => resp.json())
.then(function(data) {
    console.log('first async fetch func');
    console.log(JSON.stringify(data));

    ReactDOM.render(
        <Main 
            schemaData = {data}
            tableNames = {getUnique(data[0],"TABLE_NAME")}
        />, 
        document.getElementById('root'));
  })




// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
