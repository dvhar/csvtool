import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {postRequest,bit} from './utils.js';
import * as command from './command.js';
import * as display from './display.js';
import * as help from './help.js';
import * as topbar from './topbar.js';
import * as serviceWorker from './serviceWorker';

var bugtimer = window.performance.now() + 30000

class Main extends React.Component {
	constructor(props) {
		super(props);

		this.state = {
			topMessage : "",
			topDropdown : "nothing",
			openDirlist : {},
			saveDirlist : {},
			queryHistory: ['',],
			historyPosition : 0,
			showQuery : <></>,
			showHelp : 0,
		}
		this.topDropReset = this.topDropReset.bind(this);

		//get initial file path
		postRequest({path:"/info/",body:{}})
		.then(dat=>{
			this.setState({ openDirlist : dat.Status & bit.FP_OERROR===1 ? {Path:""} : { Path: dat.OpenPath },
							saveDirlist : dat.Status & bit.FP_OERROR===1 ? {Path:""} : { Path: dat.SavePath } });
		});

	}
	showLoadedQuery(results){
		if (results.Status & bit.DAT_ERROR){
			if (results.Message === undefined || results.Message === ""){
				alert("Could not make query or get error message from query engine");
				console.log(results);
			}else
				alert(results.Message);
		}
		else if (results.Status & bit.DAT_GOOD){
			this.setState({
				showQuery : results.Entries.map(
					tab => <display.QueryRender 
							   table = {tab} 
							   hideColumns = {new Int8Array(tab.Numcols)}
							   rows = {new Object({col:"",val:"*"})}
						   />) });
		}
	}

	submitQuery(querySpecs){
		var fullQuery = {
			Query: querySpecs.query || "", 
			FileIO: querySpecs.fileIO || 0, 
			SavePath: querySpecs.savePath || "", 
			};
		postRequest({path:"/query/",body:fullQuery}).then(dat=>{
			if ((dat.Status & bit.DAT_GOOD) && (!querySpecs.backtrack)){
				this.setState({ historyPosition : this.state.queryHistory.length,
								queryHistory : this.state.queryHistory.concat({query : dat.OriginalQuery}) });
			}
			this.showLoadedQuery(dat);
		});
	}
	sendSocket(request){
		this.ws.send(JSON.stringify(request));
	}

	viewHistory(position){
		var q = this.state.queryHistory[position];
		this.setState({ historyPosition : position });
		this.submitQuery({ query : q.query, backtrack : true});
	}
	topDropReset(e){ 
		setTimeout(()=>{
		if (!e.target.matches('.dropContent'))
			this.setState({ topDropdown : "nothing" }); 
		},50);
	}
	changeFilePath(whichPath){
		if (whichPath.type === "open")
			this.state.openDirlist.Path = whichPath.path;
		if (whichPath.type === "save")
			this.state.saveDirlist.Path = whichPath.path;
		this.forceUpdate();
	}

	render(){
	
		document.addEventListener('click',this.topDropReset);

		return (
		<>
		<topbar.TopMenuBar
			s = {this.state}
			updateTopMessage = {(message)=>this.setState({ topMessage : message })}
			submitQuery = {(query)=>this.submitQuery(query)}
			viewHistory = {(position)=>this.viewHistory(position)}
			changeTopDrop = {(section)=>this.setState({ topDropdown : section })}
			toggleHelp = {()=>{this.setState({showHelp:this.state.showHelp^1})}}
			showHelp = {this.state.showHelp}
			openDirlist = {this.state.openDirlist}
			saveDirlist = {this.state.saveDirlist}
			changeFilePath = {(path)=>this.changeFilePath(path)}
			sendSocket = {(request)=>this.sendSocket(request)}
		/>
		<help.Help
			show = {this.state.showHelp}
			toggleHelp = {()=>{this.setState({showHelp:this.state.showHelp^1})}}
		/>

		<div className="querySelect"> 
		<command.QueryBox
			s = {this.state}
			showLoadedQuery = {(results)=>this.showLoadedQuery(results)}
			submitQuery = {(query)=>this.submitQuery(query)}
			sendSocket = {(request)=>this.sendSocket(request)}
		/>
		{this.state.showQuery}
		{/*<display.TableGrid2/>*/}
		</div>
		</>
		)
	}

	componentDidMount(){
		//websocket
		var that = this;
		this.ws = new WebSocket("ws://" + document.location.host + "/socket/");
		console.log(this.ws);
		this.ws.onopen = function(e) { console.log("OPEN"); }
		this.ws.onclose = function(e) { console.log("CLOSE"); that.ws = null; } 
		this.ws.onmessage = function(e) { 
			var dat = JSON.parse(e.data);
			//console.log(dat);
			switch (dat.Type) {
				case bit.SK_PING:
					bugtimer = window.performance.now() + 5000
					break;
				case bit.SK_MSG:
					that.setState({ topMessage : dat.Text }); 
					break;
				case bit.SK_DIRLIST:
					switch (dat.Dir.Mode){
						case "open": that.setState({ openDirlist : dat.Dir });
							break;
						case "save": that.setState({ saveDirlist : dat.Dir });
							break;
					}
					break;
			}
		}
		this.ws.onerror = function(e) { console.log("ERROR: " + e.data); } 
		window.setInterval(()=>{
			if (window.performance.now() > bugtimer)
				that.setState({ topMessage : "Query Engine Disconnected!"})
		},2000);
	}
	componentWillMount() { document.title = 'CSV Query Tool' }
}

ReactDOM.render(
	<Main/>, 
	document.getElementById('root'));



// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
