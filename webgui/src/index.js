import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {postRequest,getRequest,bit} from './utils.js';
import * as command from './command.js';
import * as display from './display.js';
import * as help from './help.js';
import * as topbar from './topbar.js';
import * as serviceWorker from './serviceWorker';


class Main extends React.Component {
	constructor(props) {
		super(props);

		this.state = {
			topMessage : "",
			topDropdown : "nothing",
			openDirList : {},
			saveDirList : {},
			queryHistory: ['',],
			historyPosition : 0,
			showQuery : <></>,
			showHelp : 0,
		}
		this.topDropReset = this.topDropReset.bind(this);
		var that = this;

		//restore previous session or initialize paths
		getRequest({info:"getState"})
			.then(dat=>{
				if (dat.history) this.setState({ queryHistory : dat.history, historyPosition : dat.history.length-1 });
				if (dat.openDirList) this.setState({ openDirList : dat.openDirList });
				if (dat.saveDirList) this.setState({ saveDirList : dat.saveDirList });
				var textbox = document.getElementById("textBoxId");
				if (textbox != null)
					textbox.value = this.state.queryHistory[this.state.historyPosition].query || "";
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
				topDropdown : "nothing",
				showQuery : results.Entries.map(
					tab => <display.QueryRender 
							   table = {tab} 
							   hideColumns = {new Int8Array(tab.Numcols)}
							   rows = {new Object({col:"",val:"*"})}
						   />) });
			postRequest({path:"/info?info=setState",body:{
				haveInfo : true,
				currentQuery : document.getElementById("textBoxId").value,
				history : this.state.queryHistory,
				openDirList : this.state.openDirList,
				saveDirList : this.state.saveDirList,
			}});
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
		var textbox = document.getElementById("textBoxId");
		if (textbox != null) { textbox.value = this.state.queryHistory[position].query; }
		//this.submitQuery({ query : q.query, backtrack : true});
	}
	topDropReset(e){ 
		setTimeout(()=>{
		if (!e.target.matches('.dropContent'))
			this.setState({ topDropdown : "nothing" }); 
		},50);
	}
	changeFilePath(whichPath){
		if (whichPath.type === "open")
			this.state.openDirList.Path = whichPath.path;
		if (whichPath.type === "save")
			this.state.saveDirList.Path = whichPath.path;
		this.forceUpdate();
	}

	fileClick(request){
		postRequest({path:"/info?info=fileClick",body:{
			path : request.path,
			mode : request.mode,
		}}).then(dat=>{if (dat.Mode) {
			switch (dat.Mode){
			case "open":
				this.setState({openDirList: dat});
				break;
			case "save":
				this.setState({saveDirList: dat});
				break;
			}
		} else {
			this.setState({topMessage: "connection error"});
		}});
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
			openDirList = {this.state.openDirList}
			saveDirList = {this.state.saveDirList}
			changeFilePath = {(path)=>this.changeFilePath(path)}
			sendSocket = {(request)=>this.sendSocket(request)}
			fileClick = {(request)=>this.fileClick(request)}
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
		</div>
		</>
		)
	}

	componentDidMount(){
		//websocket
		var bugtimer = window.performance.now() + 30000
		var that = this;
		this.ws = new WebSocket("ws://" + document.location.host + "/socket/");
		console.log(this.ws);
		this.ws.onopen = function(e) { console.log("OPEN"); }
		this.ws.onclose = function(e) { console.log("CLOSE"); that.ws = null; } 
		this.ws.onmessage = function(e) { 
			var dat = JSON.parse(e.data);
			switch (dat.Type) {
			case bit.SK_PING:
				bugtimer = window.performance.now() + 20000
				break;
			case bit.SK_MSG:
				that.setState({ topMessage : dat.Text }); 
				break;
			case bit.SK_PASS:
				that.setState({ topDropdown : "passShow" });
				break;
			}
		}
		this.ws.onerror = function(e) { console.log("ERROR: " + e.data); } 
		window.setInterval(()=>{
			if (window.performance.now() > bugtimer+20000)
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
