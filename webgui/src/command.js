import React from 'react';
import {bit} from './utils.js';

export class QueryBox extends React.Component {
	constructor(props){
		super(props);
		this.state = {clicked:1}
	}
	shiftEnter(e){
		if (e.keyCode === 13 && e.shiftKey) {
			e.preventDefault();
			var query = this.textbox.value;
			this.props.submitQuery({query : query});
		}
	}
	render(){
		var arrow = <span className={this.state.clicked===1?"dim":""}>{"\u25bc"}</span>
		return (
			<div className="queryMenuContainer"> 
				<div className="dropmenu queryMenuDiv">
					<div className="dropButton queryMenuButton" onClick={()=>{this.setState({clicked:this.state.clicked^1})}}>
						<h2 className="commandtitle">Enter CSV Query {arrow}</h2>
					</div>
					<div className={`dropmenu-content queryTextContainer ${this.state.clicked===1?"show":""}`}>
					<textarea rows="10" className="queryTextEntry" id="textBoxId" placeholder={`If running multiple queries, separate them with a semicolon;`}>
					</textarea>
					<br/>
					<button className="queryRunButton dropContent" onClick={()=>{
						var query = this.textbox.value;
						this.props.submitQuery({query : query});
					}}>Submit Query</button>
					<button className="queryRunButton" onClick={()=>{ this.props.sendSocket({Type : bit.SK_STOP}); }}>End Query Early</button>
					</div>
				</div>
			</div>
		);
	}
	componentDidMount(){
		this.textbox = document.getElementById("textBoxId");
		this.textbox.onkeydown = (e)=>this.shiftEnter(e);
	}
}
