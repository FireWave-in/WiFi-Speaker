const SR=48000,CH=2,INIT=750,MAX=1200,KEY="wifiSpeakerName";
let socket=null,ctx=null,processor=null,gain=null,queue=[],packet=null,pos=0,frames=0,started=false,volume=1,muted=false;

const $=id=>document.getElementById(id);
const status=$("status"),button=$("connectButton"),buffer=$("buffer"),error=$("error");
const device=$("device"),rename=$("renameButton"),editor=$("nameEditor"),input=$("nameInput"),save=$("saveNameButton");

function detect(){
 const ua=navigator.userAgent;
 let platform=/Android/i.test(ua)?"Android":/iPhone|iPad|iPod/i.test(ua)?"iOS":/Windows/i.test(ua)?"Windows":/Macintosh/i.test(ua)?"macOS":/Linux/i.test(ua)?"Linux":"Unknown Device";
 let browser=/Edg/i.test(ua)?"Edge":/Chrome/i.test(ua)?"Chrome":/Firefox/i.test(ua)?"Firefox":/Safari/i.test(ua)?"Safari":"Unknown Browser";
 return{platform,browser};
}

const info=detect();

function getName(){
 return localStorage.getItem(KEY)?.trim()||`${info.platform} • ${info.browser}`;
}

function setName(n){
 n=n.trim()||`${info.platform} • ${info.browser}`;
 localStorage.setItem(KEY,n);
 updateName();
}

function updateName(){
 if(device)device.textContent=`📱 ${getName()}`;
}

updateName();

if(rename)rename.onclick=()=>{
 if(!editor||!input)return;
 editor.style.display="block";
 input.value=getName();
 input.focus();
};

if(save)save.onclick=()=>{
 if(!input)return;
 const n=input.value.trim();
 if(!n)return input.focus();
 setName(n);
 if(editor)editor.style.display="none";
 sendIdentification();
};

if(input)input.onkeydown=e=>{
 if(e.key==="Enter"&&save)save.click();
};

function sendIdentification(){
 if(!socket||socket.readyState!==WebSocket.OPEN)return;
 socket.send(JSON.stringify({
  type:"identify",
  name:getName(),
  platform:info.platform,
  browser:info.browser
 }));
}

if(button)button.onclick=async()=>{
 if(socket?.readyState===WebSocket.OPEN){
  socket.close();
  return;
 }

 error.textContent="";

 try{
  ctx=new(window.AudioContext||window.webkitAudioContext)({sampleRate:SR});
  await ctx.resume();

  processor=ctx.createScriptProcessor(4096,0,CH);
  processor.onaudioprocess=processAudio;

  gain=ctx.createGain();
  gain.gain.value=muted?0:volume;

  processor.connect(gain);
  gain.connect(ctx.destination);

  const protocol=location.protocol==="https:"?"wss:":"ws:";
  const url=`${protocol}//${location.hostname}:8000/audio`;

  socket=new WebSocket(url);
  socket.binaryType="arraybuffer";

  socket.onopen=()=>{
   status.textContent="Connected 🔊";
   button.textContent="Disconnect";
   sendIdentification();
   queue=[];
   packet=null;
   pos=0;
   frames=0;
   started=false;
   updateBuffer();
  };

  socket.onmessage=e=>{
   if(typeof e.data==="string"){
    control(e.data);
    return;
   }

   const pcm=new Int16Array(e.data),n=pcm.length/CH;
   const left=new Float32Array(n),right=new Float32Array(n);

   for(let i=0;i<n;i++){
    left[i]=pcm[i*2]/32768;
    right[i]=pcm[i*2+1]/32768;
   }

   queue.push({left,right});
   frames+=n;

   const max=SR*MAX/1000;

   while(frames>max){
    const p=queue.shift();
    if(!p)break;
    frames-=p.left.length;
   }

   if(!started&&frames>=SR*INIT/1000){
    started=true;
    console.log("Playback started");
   }

   updateBuffer();
  };

  socket.onerror=e=>{
   console.error(e);
   status.textContent="Connection error";
   error.textContent="Could not connect to laptop.";
  };

  socket.onclose=()=>{
   status.textContent="Disconnected";
   button.textContent="Connect";
   queue=[];
   packet=null;
   pos=0;
   frames=0;
   started=false;
   updateBuffer();
  };

 }catch(e){
  console.error(e);
  status.textContent="Audio initialization failed";
  error.textContent=e.message||String(e);
 }
};

function control(message){
 try{
  const c=JSON.parse(message);

  if(c.type==="volume"){
   volume=Math.max(0,Math.min(1,Number(c.volume)));
   if(gain)gain.gain.setTargetAtTime(
    muted?0:volume,ctx.currentTime,.01
   );
  }

  if(c.type==="mute"){
   muted=!!c.muted;
   if(gain)gain.gain.setTargetAtTime(
    muted?0:volume,ctx.currentTime,.01
   );
  }
 }catch(e){
  console.error("Invalid control message:",e);
 }
}

function processAudio(e){
 const out=e.outputBuffer;
 const left=out.getChannelData(0);
 const right=out.numberOfChannels>1?out.getChannelData(1):null;

 for(let i=0;i<left.length;i++){
  if(!started){
   left[i]=0;
   if(right)right[i]=0;
   continue;
  }

  if(!packet||pos>=packet.left.length){
   packet=queue.shift();
   pos=0;

   if(!packet){
    left[i]=0;
    if(right)right[i]=0;
    continue;
   }
  }

  left[i]=packet.left[pos];
  if(right)right[i]=packet.right[pos];

  pos++;
  frames--;
 }

 updateBuffer();
}

function updateBuffer(){
 if(!buffer)return;
 buffer.textContent=`Buffer: ${Math.max(0,Math.round(frames/SR*1000))} ms`;
}