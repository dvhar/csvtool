import React from 'react';
import {bit} from './utils.js';
import * as premades from './premades.js';



export class QuerySelect extends React.Component {
    render(){
        var sqlServerMetaDataMenu = ( <div className="queryMenuContainer"> 
                         <DropdownQueryMenu
                            title = {<h2>View database schema query{"\u25bc"}</h2>}
                            size = {premades.metaDataQueries.length}
                            //make this run multi-select queries
                            contents = {premades.metaDataQueries}
                            submit = {(query)=>this.props.submitQuery(query)}
                         />
                     </div>);

        var sqlServerCustomQueryEntry = ( <div className="queryMenuContainer"> 
                         <DropdownQueryTextbox
                            title = {<>Enter SQL Query</>}
                            submit = {(query)=>this.props.submitQuery(query)}
                            s = {this.props.s}
                            open = {false}
                         />
                     </div>);

        var csvCustomQueryEntry = ( <div className="queryMenuContainer"> 
                         <DropdownQueryTextbox
                            title = {<>Enter CSV Query</>}
                            submit = {this.props.submitQuery}
                            send = {this.props.sendSocket}
                            s = {this.props.s}
                            open = {true}
                         />
                     </div>);

        var selectors = [];

        if (this.props.s.mode === "MSSQL")
            selectors.push(sqlServerMetaDataMenu, sqlServerCustomQueryEntry);
        else
            selectors.push(csvCustomQueryEntry);

        return (
            <div className="querySelect"> 
            {selectors} 
            {this.props.showQuery} 
            </div>
        );
    }
}

//old premade queries for deprecated SQL server feature
function DropdownQueryMenu(props){
    return(
        <div className="dropmenu queryMenuDiv">
            <div className="dropButton queryMenuButton">
                {props.title}
            </div>
            <div className="dropmenu-content">
            <select size={String(props.size)} className="dropSelect" id="premadeMultiSelect" multiple>
                {props.contents.map((v,i)=><option key={i} data-key={v.key} data-idx={i}>{v.label}</option>)}
            </select>
            <button onClick={()=>{
                    var queries = "";
                    var selected = document.getElementById("premadeMultiSelect").selectedOptions;
                    for (var i in selected)
                        if (i === Number(i))
                            queries += premades.metaDataQueries[selected[i].getAttribute("data-idx")].query;
                    props.submit({query : queries});
                }}
            >Submit</button>
            </div>
        </div>
    )
}

class DropdownQueryTextbox extends React.Component {
    constructor(props){
        super(props);
        this.state = {clicked:this.props.open?1:0}
    }
    render(){
        var arrow = <span className={this.state.clicked===1?"dim":""}>{"\u25bc"}</span>
        return(
            <div className="dropmenu queryMenuDiv">
                <div className="dropButton queryMenuButton" onClick={()=>{this.setState({clicked:this.state.clicked^1})}}>
                    <h2>{this.props.title}{arrow}</h2>
                </div>
                <div className={`dropmenu-content ${this.state.clicked===1?"show":""}`}>
                <textarea rows="10" className="queryTextEntry" id="textBoxId" placeholder={`If running multiple queries, separate them with a semicolon;`}>
                </textarea>
                <br/>
                <button onClick={()=>{
                    var query = document.getElementById("textBoxId").value;
                    this.props.submit({query : query});
                }}>Submit Query</button>
                <button onClick={()=>{ this.props.send({Type : bit.SK_STOP}); }}>End Query Early</button>
                </div>
            </div>
        );
    }
    componentDidMount(){ console.log("mounted dq box"); }
}
