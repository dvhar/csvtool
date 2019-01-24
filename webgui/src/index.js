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
            <div className="tableButton1" key={idx} onClick={()=>{ this.props.changeTable(name) }}>
                {name}
            </div>
        )
    }

    render(){
        return (
            <div className="dropmenu">
                <div className="dropButton">
                Table Names
                </div>
                <div className="dropmenu-content">
                    {tableNames.map((name,i)=>{return this.tableBut(name,i)})}
                </div>
            </div>
        )
    }
}

class QueryRender extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            hideColumns : this.props.hide
        }
    }
    hideColumn(column,numeric){
        if (!numeric)
            column = colIndex(this.props.table,column)|0;
        if (this.state.hideColumns.indexOf(column)<0){
            this.state.hideColumns.push(column);
            this.forceUpdate();
            console.log(this.state.hideColumns);
            console.log(column);
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
    render(){
        return ( 
            <div> 
            <TableList
                changeTable = {(tableName)=>{this.props.changeTable(tableName)}}
            />
            <table className="table">
                    <tbody>
                    {this.row(this.props.table.Colnames,'head')}
                    {this.rows(this.props.table.Vals)}
                    </tbody>
                  </table>
            </div>
        )
    }
}

class Main extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            metaTable : tableData,
        }
    }
    changeTable(tableKey,tableName){
        this.setState({[tableKey] : getWhere(schemaData[0],"TABLE_NAME",tableName) });
    }

    render(){
        return (
            <>
            <QueryRender 
                s = {this.state}
                changeTable = {(name)=>{this.changeTable('metaTable',name)}}
                table = {this.state.metaTable}
                hide = {[0,1,2]}
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
