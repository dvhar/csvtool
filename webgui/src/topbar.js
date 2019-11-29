import React from 'react';
import {bit} from './utils.js';

export class TopMenuBar extends React.Component {
	render(){

		return (
			<div className="topBar">
			<TopDropdown
				updateTopMessage = {this.props.updateTopMessage}
				changeTopDrop = {this.props.changeTopDrop}
				currentQuery = {this.props.s.queryHistory[this.props.s.historyPosition]}
				submitQuery = {this.props.submitQuery}
				section = {this.props.s.topDropdown}
				savepath = {this.props.s.savepath}
				sendSocket = {this.props.sendSocket}
				openDirList = {this.props.openDirList}
				saveDirList = {this.props.saveDirList}
				changeSavePath = {(path)=>this.props.changeFilePath({type:"save",path:path})}
				changeOpenPath = {(path)=>this.props.changeFilePath({type:"open",path:path})}
				fileClick = {(request)=>this.props.fileClick(request)}
			/>
			<SaveButton
				dirList = {this.props.saveDirList}
				changeTopDrop = {this.props.changeTopDrop}
				fileClick = {(request)=>this.props.fileClick(request)}
			/>
			<BrowseButton
				dirList = {this.props.openDirList}
				changeTopDrop = {this.props.changeTopDrop}
				fileClick = {(request)=>this.props.fileClick(request)}
			/>
			<HelpButton
				showHelp = {this.props.showHelp}
				toggleHelp = {this.props.toggleHelp}
			/>
			<div id="topMessage" className="topMessage">{this.props.s.topMessage}</div>
			<History
				position = {this.props.s.historyPosition} 
				last = {this.props.s.queryHistory.length - 1}
				viewHistory = {this.props.viewHistory}
			/>
			</div>
		)
	}
}

class History extends React.Component {
	render(){
		return(
			<div className="historyArrows">
			{['◀ ',`${this.props.position}/${this.props.last}`,' ▶'].map((v,i)=> <span className={`${i===1?"":"arrow"}`} onClick={()=>{
				if (i === 0 && this.props.position > 1)
					this.props.viewHistory(this.props.position - 1);
				if (i === 2 && this.props.position < this.props.last)
					this.props.viewHistory(this.props.position + 1);
			}}>{v}</span>)}
			</div>
		)
	}
}

class TopDropdown extends React.Component {

	render(){
		var dropdownContents = {
			nothing : <></>,
			saveShow : ( <Browser
							dirList = {this.props.saveDirList}
							fileClick = {(request)=>this.props.fileClick(request)}
							changePath = {this.props.changeSavePath}
							changeTopDrop = {this.props.changeTopDrop}
							submitQuery = {this.props.submitQuery}
							currentQuery = {this.props.currentQuery}
							type = {"save"}
						/> ),
			browseShow : ( <Browser
							dirList = {this.props.openDirList}
							updateTopMessage = {this.props.updateTopMessage}
							changeTopDrop = {this.props.changeTopDrop}
							fileClick = {(request)=>this.props.fileClick(request)}
							changePath = {this.props.changeOpenPath}
							type = {"open"}
						/> ),
			passShow: <PassPrompt
							changeTopDrop = {this.props.changeTopDrop}
							send = {this.props.sendSocket}
						/>

		}
		return dropdownContents[this.props.section];
	}
}

//each csv file line in file browser
class FileSelector extends React.Component {
	render() {
		return (
		<>
			<span className="dropContent browseFile browseEntry" onDoubleClick={()=>{
				if (this.props.type === 'open') {
					var qtext = document.getElementById("textBoxId");
					var start = qtext.value.substring(0,qtext.selectionStart);
					var end = qtext.value.substring(qtext.selectionEnd, 10000000);
					qtext.value = start +" '"+ this.props.path +"' "+ end;
					this.props.updateTopMessage("Added file to query");
					this.props.changeTopDrop("nothing");
				}
			}} 
			>{this.props.path}</span>
		</>
		)
	}
}
//file browser
class Browser extends React.Component {
	constructor(props) {
		super(props);
		this.handleChange = this.handleChange.bind(this);
		this.state = {
			innerBoxId : Math.random(),
			outterBoxId : Math.random(),
			currentDirId : Math.random(),
		};
	}
	clickPath(path){
		//this.props.send({Type : bit.SK_FILECLICK, Text : path, Mode : this.props.type});
		this.props.fileClick({path : path, mode : this.props.type});
	}
	dirList(){
		if (this.props.dirList.Dirs) return (
		this.props.dirList.Dirs.map(path => <span className="dropContent browseDir browseEntry" onClick={()=>this.clickPath(path)}>{path}</span>)
		);
	}
	filelist(){
		if (this.props.dirList.Files) return (
		this.props.dirList.Files.map(path => <FileSelector 
			path={path}
			changeTopDrop = {this.props.changeTopDrop}
			type={this.props.type}
			updateTopMessage = {this.props.updateTopMessage}
		/>)
		);
	}
	handleChange(e){
		this.props.changePath(e.target.value);
	}

	render(){
		var header = [];
		if (this.props.type === "open")
			header.push( <><span>Double click a file you want to query</span><br/></> );
		if (this.props.type === "save")
			header.push( <><span>Save queries on page to their own csv file. A number will be added to file name if more than 1.</span>
				<button className="saveButton topButton popButton dropContent" onClick={()=>{
					var path = document.getElementById(this.state.currentDirId).value;
					this.props.submitQuery({query : this.props.currentQuery.query, fileIO : bit.F_SAVE|bit.F_CSV, savePath : path});
				}}>save</button><br/></> );

		return (
		<div id={this.state.outterBoxId} className="fileSelectShow fileBrowser dropContent">
			{header}
			<input className="dropContent browseDir browseCurrent" id={this.state.currentDirId} value={this.props.dirList.Path} onChange={this.handleChange}/>
			<div className="browseDirs dropContent" id={this.state.innerBoxId}>
			<span className="dropContent browseDir browseEntry" onClick={()=>{ this.clickPath(this.props.dirList.Parent);
			}}><b>←</b></span>
			{this.dirList()}
			{this.filelist()}
			</div>
		</div>
		)
	}
	componentDidUpdate(){
		const innerBox = document.getElementById(this.state.innerBoxId);
		const outterBox = document.getElementById(this.state.outterBoxId);
		var size = outterBox.offsetHeight;
		if (innerBox.offsetHeight > 480){
			innerBox.style.height = `${size-80}px`;
		}
	}
	componentDidMount(){
		var that = this;
		const dirText = document.getElementById(this.state.currentDirId);
		dirText.addEventListener("keyup", function(event) {
			if (event.key === "Enter") {
				that.clickPath(that.props.dirList.Path)
			}
		});
	}
}
class SaveButton extends React.Component {
	toggleForm(){
		this.props.fileClick({path : this.props.dirList.Path, mode : "save"});
		this.props.changeTopDrop("saveShow");
	}
	render(){
		return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.toggleForm()}>Save</button>)
	}
}

class BrowseButton extends React.Component {
	toggleForm(){
		this.props.fileClick({path : this.props.dirList.Path, mode : "open"});
		this.props.changeTopDrop("browseShow");
	}
	render(){
		return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.toggleForm()}>Browse Files</button>)
	}
}

class HelpButton extends React.Component {
	render(){
		var label = "Help";
		if (this.props.showHelp) label = "Hide Help";
		return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.props.toggleHelp()}>{label}</button>)
	}
}

class PassPrompt extends React.Component {
	render() {
		//<div className="passPopup">
		return (
		<div className="fileSelectShow dropContent passPopup">
		<h2 className="passPromtText dropContent">Enter encryption password</h2>
		<input type="password" id="passInput" className="dropContent"></input>
		<button className="topButton popButton dropContent" onClick={()=>{
			this.props.send({Type : bit.SK_PASS, Text : document.getElementById("passInput").value});
			}} >Submit</button>
		</div>
		);
	}
	componentDidMount(){
		var passbox = document.getElementById("passInput");
		passbox.onkeydown = (e)=>{
			if (e.keyCode === 13) {
				this.props.send({Type : bit.SK_PASS, Text : document.getElementById("passInput").value});
			}
		};
	}
}
