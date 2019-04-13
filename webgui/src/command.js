import React from 'react';
import {bit} from './utils.js';



export class QuerySelect extends React.Component {
    render(){

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

        selectors.push(csvCustomQueryEntry);

        return (
            <div className="querySelect"> 
            {selectors} 
            {this.props.showQuery} 
            </div>
        );
    }
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
                    <h2 className="commandtitle">{this.props.title}{arrow}</h2>
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
