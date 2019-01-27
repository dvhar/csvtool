import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {log,getData,getUnique,getWhere,colIndex} from './utils.js';
import * as serviceWorker from './serviceWorker';

var schemaData = getData();
var tableNames =  getUnique(schemaData[0],"TABLE_NAME");
var tableData = getWhere(schemaData[0],"TABLE_NAME",tableNames[0]);

class TableList extends React.Component {
    tableBut(name,idx){
        return (
            <option className="tableButton1" key={idx} onClick={()=>{ this.props.changeTable(name) }}>
                {name}
            </option>
        )
    }

    render(){
        console.log(this.props.section);
        return (
            <div className="dropmenu">
                <div className="dropButton">
                {this.props.section}
                </div>
                <select size="10" className="dropmenu-content">
                    {tableNames.map((name,i)=>{return this.tableBut(name,i)})}
                </select>
            </div>
        )
    }
}

class QueryRender extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            hideColumns : this.props.hide,
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
    cell(name,idx,type){
        if (this.state.hideColumns.indexOf(idx)<0)
            if (type === 'head')
                return( <th key={idx} className="tableCell" onClick={()=>{this.hideColumn(idx,true)}}> {name} </th>)
            else
                return( <td key={idx} className="tableCell"> {name} </td>)
    }
    row(row,type){
        return( <tr className="tableRow"> {row.map((name,idx)=>{ return this.cell(name,idx,type); })} </tr> )
    }
    rows(rows){
        return( rows.map((row,i)=>{return this.row(row)}) )
    }
    changeTable(section){
        this.setState({section : section,
                       table : getWhere(schemaData[0],"TABLE_NAME",section) });
    }
    render(){
        console.log(this.state.section);
        return ( 
        <div className="viewContainer">
            <TableList 
                changeTable = {(section)=>{this.changeTable(section)}}
                section = {this.state.section}
            />
            <div className="filler">
            </div>
            <div className="tableDiv"> 
            <table className="table">
                {this.row(this.state.table.Colnames,'head')}
                <tbody>
                {this.rows(this.state.table.Vals)}
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
            metaTable : schemaData[0],
        }
    }

    render(){
        return (
            <>
            <QueryRender 
                s = {this.state}
                table = {this.state.metaTable}
                section = {tableNames[0]}
                hide = {[]}
            />
            <QueryRender 
                s = {this.state}
                table = {this.state.metaTable}
                section = {tableNames[1]}
                hide = {[]}
            />
            </>
        )
    }
}

ReactDOM.render(<Main />, document.getElementById('root'));

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
