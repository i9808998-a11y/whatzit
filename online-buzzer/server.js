
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { URL } = require("url");

// Minimal zero-dependency .env loader for local deployment. Existing process env wins.
try {
  const envFile = path.join(__dirname, ".env");
  if (fs.existsSync(envFile)) {
    for (const raw of fs.readFileSync(envFile, "utf8").split(/\r?\n/)) {
      const line = raw.trim(); if (!line || line.startsWith("#")) continue;
      const i = line.indexOf("="); if (i < 1) continue;
      const k = line.slice(0, i).trim(), v = line.slice(i + 1).trim();
      if (!(k in process.env)) process.env[k] = v.replace(/^([\"'])(.*)\1$/, "$2");
    }
  }
} catch (e) { console.warn("Could not read .env:", e.message); }

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "0.0.0.0";
const ADMIN_KEY = process.env.ADMIN_KEY || "CHANGE_ME_BEFORE_DEPLOY";
const DB_FILE = process.env.DB_FILE || path.join(__dirname, "database", "buzzer-data.json");
const PUBLIC_DIR = path.join(__dirname, "public");

fs.mkdirSync(path.dirname(DB_FILE), {recursive:true});

function now(){ return Date.now(); }
function uid(prefix){ return prefix+"_"+crypto.randomBytes(10).toString("hex"); }
function secret(){ return crypto.randomBytes(24).toString("hex"); }
function roomCode(){ return crypto.randomBytes(3).toString("hex").toUpperCase(); }
function hash(v){ return crypto.createHash("sha256").update(String(v)).digest("hex"); }

function loadDb(){
  try{
    const x=JSON.parse(fs.readFileSync(DB_FILE,"utf8"));
    if(x && x.rooms && x.teams && x.events) return x;
  }catch{}
  return {rooms:{},teams:{},events:[]};
}
let db=loadDb();
for(const r of Object.values(db.rooms||{})){
  if(r.finalized==null) r.finalized=false;
  if(r.finalizedAt==null) r.finalizedAt=null;
  if(r.finalSnapshot==null) r.finalSnapshot=null;
  if(r.buzzerRemainingMs==null) r.buzzerRemainingMs=null;
}
let writeQueue=Promise.resolve();
function saveDb(){
  const snapshot=JSON.stringify(db,null,2);
  const tmp=DB_FILE+".tmp";
  writeQueue=writeQueue.then(async()=>{
    await fs.promises.writeFile(tmp,snapshot,"utf8");
    await fs.promises.rename(tmp,DB_FILE);
  }).catch(err=>console.error("DB write failed:",err));
  return writeQueue;
}

const clients = new Map(); // roomId -> Set of {res,role,teamId}
function roomClients(id){ if(!clients.has(id)) clients.set(id,new Set()); return clients.get(id); }
function sendSse(c,type,data){
  try{ c.res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`); }catch{}
}
function broadcast(id,type,data,filter){
  for(const c of [...roomClients(id)]) if(!filter || filter(c)) sendSse(c,type,data);
}
function onlineCount(id){
  let n=0; for(const c of roomClients(id)) if(c.role==="student" && c.teamId) n++;
  return n;
}
function roomState(r){
  return {
    id:r.id,name:r.name,totalQuestions:r.totalQuestions,
    status:r.status,currentQuestion:r.currentQuestion||null,
    buzzerOpen:!!r.buzzerOpen,buzzerExpiresAt:r.buzzerExpiresAt||null,
    buzzerStartedAt:r.buzzerStartedAt||null,buzzerDurationMs:r.buzzerDurationMs||null,
    buzzerRemainingMs:r.buzzerRemainingMs||null,
    currentRoundId:r.currentRoundId||null,onlineTeams:onlineCount(r.id),
    finalized:!!r.finalized,finalizedAt:r.finalizedAt||null
  };
}
function pendingEvent(r){
  if(!r.currentRoundId) return null;
  return db.events.find(e=>e.roomId===r.id && e.roundId===r.currentRoundId && !e.result) || null;
}
function teamForSession(roomId,teamId,sessionToken){
  const t=db.teams[teamId];
  return t && t.roomId===roomId && t.enabled && hash(sessionToken)===t.sessionTokenHash ? t : null;
}
function writeJson(res,status,obj,headers={}){
  const b=Buffer.from(JSON.stringify(obj));
  res.writeHead(status,{"Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store","Access-Control-Allow-Origin":"*",...headers,"Content-Length":b.length});
  res.end(b);
  return true;
}
function readBody(req){
  return new Promise((resolve,reject)=>{
    let s=""; req.on("data",c=>{s+=c;if(s.length>1_000_000) reject(new Error("Payload too large"));});
    req.on("end",()=>{try{resolve(s?JSON.parse(s):{})}catch{reject(new Error("Invalid JSON"))}});
    req.on("error",reject);
  });
}
function authAdmin(req){ return String(req.headers["x-admin-key"]||"")===ADMIN_KEY; }
function authControl(room,token){ return room && token && hash(token)===room.controlTokenHash; }

function participants(id){
  return Object.values(db.teams).filter(t=>t.roomId===id && t.enabled).sort((a,b)=>a.teamName.localeCompare(b.teamName));
}
function publicTeam(t){
  return {id:t.id,teamName:t.teamName,
    student1Name:t.student1Name,student1School:t.student1School,student1Standard:t.student1Standard,
    student2Name:t.student2Name,student2School:t.student2School,student2Standard:t.student2Standard,
    enabled:t.enabled};
}
function resultRows(id){
  return db.events.filter(e=>e.roomId===id).sort((a,b)=>a.questionNumber-b.questionNumber || a.serverTimestamp-b.serverTimestamp).map(e=>{
    const t=db.teams[e.teamId]||{};
    return {...e,
      teamName:t.teamName||"",student1Name:t.student1Name||"",student1School:t.student1School||"",student1Standard:t.student1Standard||"",
      student2Name:t.student2Name||"",student2School:t.student2School||"",student2Standard:t.student2Standard||""};
  });
}
function ranking(id){
  const r=db.rooms[id];
  const teams=participants(id).map(t=>{
    const ev=db.events.filter(e=>e.roomId===id&&e.teamId===t.id&&e.result);
    const correct=ev.filter(e=>e.result==="CORRECT").length;
    const wrong=ev.filter(e=>e.result==="WRONG").length;
    const points=ev.reduce((s,e)=>s+(Number(e.points)||0),0);
    const correctBuzz=ev.filter(e=>e.result==="CORRECT").reduce((s,e)=>s+(Number(e.elapsedMs)||0),0);
    const totalBuzz=ev.reduce((s,e)=>s+(Number(e.elapsedMs)||0),0);
    return {teamId:t.id,teamName:t.teamName,student1Name:t.student1Name,student1School:t.student1School,student1Standard:t.student1Standard,
      student2Name:t.student2Name,student2School:t.student2School,student2Standard:t.student2Standard,correct,wrong,points,
      correctBuzzTimeMs:correctBuzz,totalBuzzTimeMs:totalBuzz,buzzCount:ev.length};
  });
  teams.sort((a,b)=>b.points-a.points||a.correctBuzzTimeMs-b.correctBuzzTimeMs||a.totalBuzzTimeMs-b.totalBuzzTimeMs||a.teamName.localeCompare(b.teamName));
  let lastKey=null,rank=0;
  return teams.map((x,i)=>{const k=`${x.points}|${x.correctBuzzTimeMs}|${x.totalBuzzTimeMs}`;if(k!==lastKey){rank=i+1;lastKey=k}return {...x,rank};});
}
function setClosed(r,status){r.status=status;r.buzzerOpen=false;r.buzzerStartedAt=null;r.buzzerDurationMs=null;r.buzzerExpiresAt=null;r.buzzerRemainingMs=null;r.updatedAt=now();}
function publicEvent(e){
  if(!e) return null;
  const t=db.teams[e.teamId]||{};
  return {id:e.id,eventId:e.id,roomId:e.roomId,roundId:e.roundId,questionNumber:e.questionNumber,teamId:e.teamId,
    teamName:t.teamName||e.teamName||"",student1Name:t.student1Name||"",student1School:t.student1School||"",student1Standard:t.student1Standard||"",
    student2Name:t.student2Name||"",student2School:t.student2School||"",student2Standard:t.student2Standard||"",
    serverTimestamp:e.serverTimestamp,elapsedMs:e.elapsedMs,result:e.result||null,points:e.points,evaluatedAt:e.evaluatedAt||null};
}

async function handleApi(req,res,u){
  const pathname=u.pathname;
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Headers","Content-Type,X-Admin-Key");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  if(req.method==="OPTIONS"){res.writeHead(204);res.end();return true;}

  if(pathname==="/api/health" && req.method==="GET"){writeJson(res,200,{ok:true,service:"techastra-buzzer",time:now()});return true;}
  if(pathname==="/api/admin/login" && req.method==="POST"){
    try{const b=await readBody(req);if(String(b.key||"")!==ADMIN_KEY)return writeJson(res,401,{ok:false,error:"Invalid admin key"});writeJson(res,200,{ok:true});}
    catch(e){writeJson(res,400,{ok:false,error:e.message})}return true;
  }
  if(pathname==="/api/rooms" && req.method==="POST"){
    if(!authAdmin(req))return writeJson(res,401,{ok:false,error:"Unauthorized"});
    try{
      const b=await readBody(req);const id=String(b.roomId||"").trim().toUpperCase()||roomCode();
      if(!/^[A-Z0-9_-]{4,20}$/.test(id))return writeJson(res,400,{ok:false,error:"Room ID must be 4–20 letters, numbers, _ or -."});
      if(db.rooms[id])return writeJson(res,409,{ok:false,error:"Room already exists"});
      const control=secret(), t=now(), total=Math.max(1,Math.min(1000,Number(b.totalQuestions||30)));
      const room={id,name:String(b.name||"TECHASTRA JR’26 • WHATZIT?").trim().slice(0,120),controlTokenHash:hash(control),totalQuestions:total,
        correctPoints:Math.max(0,Number(b.correctPoints??1)),wrongPoints:Math.min(0,Number(b.wrongPoints??0)),status:"WAITING",currentQuestion:null,
        currentRoundId:null,buzzerOpen:false,buzzerExpiresAt:null,buzzerStartedAt:null,buzzerDurationMs:null,buzzerRemainingMs:null,finalized:false,finalizedAt:null,finalSnapshot:null,createdAt:t,updatedAt:t};
      db.rooms[id]=room;await saveDb();writeJson(res,200,{ok:true,room:roomState(room),controlToken:control});
    }catch(e){writeJson(res,400,{ok:false,error:e.message})}
    return true;
  }
  const m=pathname.match(/^\/api\/rooms\/([^/]+)$/);
  if(m && req.method==="GET"){
    const r=db.rooms[decodeURIComponent(m[1]).toUpperCase()];if(!r)return writeJson(res,404,{ok:false,error:"Room not found"});
    return writeJson(res,200,{ok:true,room:roomState(r)});
  }
  const tm=pathname.match(/^\/api\/rooms\/([^/]+)\/teams$/);
  if(tm && req.method==="POST"){
    const id=decodeURIComponent(tm[1]).toUpperCase(),r=db.rooms[id];if(!r)return writeJson(res,404,{ok:false,error:"Room not found"});
    try{
      const b=await readBody(req), fs=["teamName","student1Name","student1School","student1Standard","student2Name","student2School","student2Standard"];
      for(const f of fs)if(!String(b[f]??"").trim())return writeJson(res,400,{ok:false,error:`${f} is required`});
      const values=Object.fromEntries(fs.map(f=>[f,String(b[f]).trim().slice(0,120)]));
      if(participants(id).some(t=>t.teamName.toLowerCase()===values.teamName.toLowerCase()))return writeJson(res,409,{ok:false,error:"A team with this team name is already registered in this room."});
      const idt=uid("team"),sess=secret(),t=now();
      db.teams[idt]={id:idt,roomId:id,...values,sessionTokenHash:hash(sess),enabled:true,createdAt:t,updatedAt:t};
      await saveDb();writeJson(res,200,{ok:true,team:publicTeam(db.teams[idt]),sessionToken:sess,room:roomState(r)});
    }catch(e){writeJson(res,400,{ok:false,error:e.message})}
    return true;
  }
  const bm=pathname.match(/^\/api\/rooms\/([^/]+)\/buzz$/);
  if(bm && req.method==="POST"){
    const id=decodeURIComponent(bm[1]).toUpperCase(),r=db.rooms[id];if(!r)return writeJson(res,404,{ok:false,error:"Room not found"});
    try{
      const b=await readBody(req),t=teamForSession(id,String(b.teamId||""),String(b.sessionToken||""));
      if(!t)return writeJson(res,401,{ok:false,error:"Invalid team session"});
      const submittedRoundId=String(b.roundId||"");
      if(!submittedRoundId || submittedRoundId!==String(r.currentRoundId||""))return writeJson(res,409,{ok:false,error:"STALE ROUND"});
      if(r.finalized)return writeJson(res,409,{ok:false,error:"Final results are already locked."});
      const open=r.buzzerOpen && r.status==="BUZZER_OPEN" && r.currentRoundId && r.buzzerExpiresAt && now()<r.buzzerExpiresAt;
      if(pendingEvent(r))return writeJson(res,409,{ok:false,error:"ROUND ALREADY BUZZED"});
      if(!open){
        if(r.buzzerExpiresAt && now()>=r.buzzerExpiresAt)return writeJson(res,409,{ok:false,error:"TIME EXPIRED"});
        return writeJson(res,409,{ok:false,error:"BUZZER NOT ACTIVE"});
      }
      // The server remains authoritative even if the organizer SSE connection
      // briefly drops. Persist the valid first buzz so the organizer can reconnect
      // and receive it as a pending event instead of rejecting the student.
      const ts=now(),elapsedMs=Math.max(0,ts-r.buzzerStartedAt),remainingMs=Math.max(0,r.buzzerExpiresAt-ts);
      const ev={id:uid("buzz"),roomId:id,roundId:r.currentRoundId,questionNumber:r.currentQuestion,teamId:t.id,
        serverTimestamp:ts,elapsedMs,result:null,points:null,evaluatedAt:null};
      db.events.push(ev);
      // Lock every student immediately, but KEEP the round alive for organizer evaluation.
      r.status="AWAITING_EVALUATION";r.buzzerOpen=false;r.buzzerRemainingMs=remainingMs;r.updatedAt=ts;
      await saveDb();
      const payload=publicEvent(ev);
      broadcast(id,"buzzer:pressed",payload);broadcast(id,"room:state",roomState(r));
      return writeJson(res,200,{ok:true,event:payload});
    }catch(e){writeJson(res,400,{ok:false,error:e.message})}
    return true;
  }
  const am=pathname.match(/^\/api\/admin\/rooms\/([^/]+)\/(teams|results|ranking)$/);
  if(am && req.method==="GET"){
    if(!authAdmin(req))return writeJson(res,401,{ok:false,error:"Unauthorized"});
    const id=decodeURIComponent(am[1]).toUpperCase(),r=db.rooms[id];if(!r)return writeJson(res,404,{ok:false,error:"Room not found"});
    if(am[2]==="teams")return writeJson(res,200,{ok:true,teams:participants(id).map(t=>({...publicTeam(t),connected:[...roomClients(id)].some(c=>c.role==="student"&&c.teamId===t.id) }))});
    if(am[2]==="results")return writeJson(res,200,{ok:true,results:resultRows(id)});
    return writeJson(res,200,{ok:true,ranking:ranking(id),totalQuestions:r.totalQuestions});
  }
  const fm=pathname.match(/^\/api\/admin\/rooms\/([^/]+)\/finalize$/);
  if(fm && req.method==="POST"){
    if(!authAdmin(req))return writeJson(res,401,{ok:false,error:"Unauthorized"});
    const id=decodeURIComponent(fm[1]).toUpperCase(),r=db.rooms[id];if(!r)return writeJson(res,404,{ok:false,error:"Room not found"});
    if(r.finalized)return writeJson(res,409,{ok:false,error:"FINAL RESULTS ALREADY LOCKED"});
    if(pendingEvent(r))return writeJson(res,409,{ok:false,error:"PENDING BUZZER EVALUATION"});
    const last=db.events.filter(e=>e.roomId===id&&e.questionNumber===r.totalQuestions&&e.result).sort((a,b)=>b.evaluatedAt-a.evaluatedAt)[0];
    if(!last)return writeJson(res,409,{ok:false,error:`Final question Q${r.totalQuestions} has not been evaluated yet.`});
    const finalizedAt=now();r.finalized=true;r.finalizedAt=finalizedAt;r.status="FINALIZED";r.finalSnapshot={finalizedAt,ranking:ranking(id)};r.updatedAt=finalizedAt;await saveDb();
    broadcast(id,"room:state",roomState(r));broadcast(id,"event:finalized",{finalizedAt,ranking:r.finalSnapshot.ranking});
    return writeJson(res,200,{ok:true,room:roomState(r),ranking:r.finalSnapshot.ranking});
  }
  const em=pathname.match(/^\/api\/admin\/rooms\/([^/]+)\/export\/(participants|results|final-results|complete-event-report)$/);
  if(em && req.method==="GET"){
    if(!authAdmin(req))return writeJson(res,401,{ok:false,error:"Unauthorized"});
    const id=decodeURIComponent(em[1]).toUpperCase(),r=db.rooms[id];if(!r)return writeJson(res,404,{ok:false,error:"Room not found"});
    let kind=em[2], rows, header;
    if(kind==="participants"){
      header=["Team Name","Student 1 Name","Student 1 School","Student 1 Standard","Student 2 Name","Student 2 School","Student 2 Standard","Registered At"];
      rows=participants(id).map(t=>[t.teamName,t.student1Name,t.student1School,t.student1Standard,t.student2Name,t.student2School,t.student2Standard,new Date(t.createdAt).toISOString()]);
    }else if(kind==="results"){
      header=["Question","Team","Student 1","Student 1 School","Student 1 Standard","Student 2","Student 2 School","Student 2 Standard","Buzz Timestamp","Elapsed Seconds","Result","Points","Round ID"];
      rows=resultRows(id).map(e=>[e.questionNumber,e.teamName,e.student1Name,e.student1School,e.student1Standard,e.student2Name,e.student2School,e.student2Standard,new Date(e.serverTimestamp).toISOString(),(e.elapsedMs/1000).toFixed(3),e.result||"PENDING",e.points??"",e.roundId]);
    }else if(kind==="final-results"){
      header=["Rank","Team","Student 1","Student 1 School","Student 1 Standard","Student 2","Student 2 School","Student 2 Standard","Correct","Wrong","Points","Correct Buzz Time Seconds","Total Buzz Time Seconds"];
      rows=ranking(id).map(x=>[x.rank,x.teamName,x.student1Name,x.student1School,x.student1Standard,x.student2Name,x.student2School,x.student2Standard,x.correct,x.wrong,x.points,(x.correctBuzzTimeMs/1000).toFixed(3),(x.totalBuzzTimeMs/1000).toFixed(3)]);
    }else{
      header=["Section","Field","Value"];
      const rank=ranking(id);
      rows=[
        ["EVENT","Room ID",r.id],["EVENT","Room Name",r.name],["EVENT","Created At",new Date(r.createdAt).toISOString()],["EVENT","Finalized At",r.finalizedAt?new Date(r.finalizedAt).toISOString():""],["EVENT","Total Questions",r.totalQuestions],["EVENT","Team Count",participants(id).length],
        ...participants(id).map(t=>["PARTICIPANT",t.teamName,`${t.student1Name} | ${t.student1School} | ${t.student1Standard} | ${t.student2Name} | ${t.student2School} | ${t.student2Standard}`]),
        ...resultRows(id).map(e=>["BUZZER EVENT",`Q${e.questionNumber} ${e.teamName}`,`${e.result||"PENDING"} | ${e.points??""} | ${new Date(e.serverTimestamp).toISOString()} | ${(e.elapsedMs/1000).toFixed(3)}s | ${e.roundId}`]),
        ...rank.map(x=>["FINAL RANKING",x.rank,`${x.teamName} | ${x.points} pts | ${x.correct} correct | ${x.wrong} wrong | ${(x.correctBuzzTimeMs/1000).toFixed(3)}s correct buzz time | ${(x.totalBuzzTimeMs/1000).toFixed(3)}s total buzz time`])
      ];
    }
    const esc=v=>{const s=String(v??"");return /[",\n]/.test(s)?`"${s.replace(/"/g,'""')}"`:s};
    const csv=[header,...rows].map(row=>row.map(esc).join(",")).join("\n");
    res.writeHead(200,{"Content-Type":"text/csv; charset=utf-8","Content-Disposition":`attachment; filename="${id}_${kind}.csv"`,"Access-Control-Allow-Origin":"*"});
    res.end(csv);return true;
  }
  const sm=pathname.match(/^\/api\/control\/state$/);
  if(sm && req.method==="GET"){
    const id=String(new URL(req.url, `http://${req.headers.host||"localhost"}`).searchParams.get("roomId")||"").toUpperCase();
    const token=String(new URL(req.url, `http://${req.headers.host||"localhost"}`).searchParams.get("token")||"");
    const r=db.rooms[id];
    if(!r || !authControl(r,token)) return writeJson(res,401,{ok:false,error:"Organizer authorization failed"});
    return writeJson(res,200,{ok:true,room:roomState(r),pendingEvent:publicEvent(pendingEvent(r))});
  }
  const cm=pathname.match(/^\/api\/control\/(open|close|sync|result)$/);
  if(cm && req.method==="POST"){
    try{
      const b=await readBody(req),id=String(b.roomId||"").toUpperCase(),r=db.rooms[id];if(!r||!authControl(r,String(b.controlToken||"")))return writeJson(res,401,{ok:false,error:"Organizer authorization failed"});
      const type=cm[1];
      if(type==="open"){
        if(r.finalized)return writeJson(res,409,{ok:false,error:"Final results are already locked."});
        const q=Math.max(1,Number(b.questionNumber||1)),duration=Math.max(1,Number(b.remainingMs||0));
        if(r.status==="AWAITING_EVALUATION" || pendingEvent(r))return writeJson(res,409,{ok:false,error:"PENDING BUZZER EVALUATION"});
        const started=now(),roundId=String(b.roundId||`${id}-Q${q}-${started}`);
        Object.assign(r,{status:"BUZZER_OPEN",currentQuestion:q,currentRoundId:roundId,buzzerOpen:true,buzzerStartedAt:started,buzzerDurationMs:duration,buzzerExpiresAt:started+duration,buzzerRemainingMs:duration,updatedAt:started});
        await saveDb();broadcast(id,"buzzer:opened",{questionNumber:q,roundId,expiresAt:r.buzzerExpiresAt,durationMs:duration});broadcast(id,"room:state",roomState(r));return writeJson(res,200,{ok:true,room:roomState(r)});
      }
      if(type==="close"){
        // A first buzzer is already locked server-side and must remain available
        // for CORRECT/WRONG evaluation. Never let a late organizer close/sync
        // request destroy an awaiting-evaluation round.
        if(pendingEvent(r)) return writeJson(res,409,{ok:false,error:"PENDING BUZZER EVALUATION"});
        const reason=String(b.reason||"CLOSED").toUpperCase();
        setClosed(r,reason==="FINISH"?"COMPLETED":reason);
        r.currentRoundId=r.currentRoundId||null;
        await saveDb();
        broadcast(id,"buzzer:closed",{reason:b.reason||"closed"});
        broadcast(id,"room:state",roomState(r));
        return writeJson(res,200,{ok:true,room:roomState(r)});
      }
      if(type==="sync"){
        r.totalQuestions=Math.max(1,Number(b.totalQuestions||r.totalQuestions));const q=Math.max(1,Number(b.questionNumber||1)),phase=String(b.phase||"READY"),remaining=Math.max(0,Number(b.remainingMs||0));
        const p=pendingEvent(r);
        if(p){
          // Never let sync/polling reopen or destroy a round awaiting evaluation.
          r.status="AWAITING_EVALUATION";r.buzzerOpen=false;
        }else if(phase==="ACTIVE"&&remaining>0){
          const incomingRoundId=String(b.roundId||"");
          const sameActiveRound=r.buzzerOpen && r.status==="BUZZER_OPEN" && r.currentQuestion===q && (!incomingRoundId || incomingRoundId===String(r.currentRoundId));
          if(!sameActiveRound){
            const started=now(),roundId=incomingRoundId || `${id}-Q${q}-${started}`;
            Object.assign(r,{status:"BUZZER_OPEN",currentQuestion:q,currentRoundId:roundId,buzzerOpen:true,buzzerStartedAt:started,buzzerDurationMs:remaining,buzzerExpiresAt:started+remaining,buzzerRemainingMs:remaining,updatedAt:started});
            broadcast(id,"buzzer:opened",{questionNumber:q,roundId,expiresAt:r.buzzerExpiresAt,durationMs:remaining});
          }
        }else if(phase!=="ACTIVE"){
          const incomingRoundId=String(b.roundId||"");
          const sameRound=!r.currentRoundId || !incomingRoundId || incomingRoundId===String(r.currentRoundId);
          if(sameRound && !r.buzzerOpen){
            setClosed(r,phase==="REVEALED"?"REVEALED":phase==="TIME_UP"?"TIME_UP":"WAITING");
          }
        }
        await saveDb();broadcast(id,"room:state",roomState(r));return writeJson(res,200,{ok:true,room:roomState(r)});
      }
      if(type==="result"){
        const ev=db.events.find(x=>x.id===String(b.eventId||"")&&x.roomId===id),result=String(b.result||"").toUpperCase();
        if(!ev||ev.result)return writeJson(res,409,{ok:false,error:"Buzzer event is missing or already evaluated."});
        if(r.status!=="AWAITING_EVALUATION" || r.currentQuestion!==ev.questionNumber || r.currentRoundId!==ev.roundId)return writeJson(res,409,{ok:false,error:"This buzzer round is no longer awaiting evaluation."});
        if(!["CORRECT","WRONG"].includes(result)||ev.roundId!==String(b.roundId||ev.roundId))return writeJson(res,400,{ok:false,error:"Invalid or stale result."});
        const points=result==="CORRECT"?r.correctPoints:r.wrongPoints;ev.result=result;ev.points=points;ev.evaluatedAt=now();
        r.status="EVALUATED";r.buzzerOpen=false;r.buzzerRemainingMs=r.buzzerRemainingMs??0;r.updatedAt=ev.evaluatedAt;
        await saveDb();
        broadcast(id,"buzzer:result",{eventId:ev.id,questionNumber:ev.questionNumber,teamId:ev.teamId,result,points,roundId:ev.roundId});broadcast(id,"room:state",roomState(r));return writeJson(res,200,{ok:true,event:publicEvent(ev)});
      }
    }catch(e){writeJson(res,400,{ok:false,error:e.message})}
    return true;
  }
  return false;
}

function serveStatic(req,res,u){
  let filePath=u.pathname==="/" ? path.join(PUBLIC_DIR,"index.html") : path.join(PUBLIC_DIR,u.pathname.replace(/^\/+/,""));
  filePath=path.normalize(filePath);
  if(!filePath.startsWith(PUBLIC_DIR)){res.writeHead(403);res.end("Forbidden");return}
  fs.readFile(filePath,(err,data)=>{
    if(err){res.writeHead(404,{"Content-Type":"text/plain"});res.end("Not found");return}
    const ext=path.extname(filePath).toLowerCase(),types={".html":"text/html; charset=utf-8",".css":"text/css; charset=utf-8",".js":"application/javascript; charset=utf-8",".json":"application/json; charset=utf-8"};
    res.writeHead(200,{"Content-Type":types[ext]||"application/octet-stream","Cache-Control":ext===".html"?"no-store":"public, max-age=3600"});
    res.end(data);
  });
}

const server=http.createServer(async(req,res)=>{
  try{
    const u=new URL(req.url,`http://${req.headers.host||"localhost"}`);
    if(u.pathname.startsWith("/api/")){const handled=await handleApi(req,res,u);if(handled)return;}
    if(u.pathname.startsWith("/stream")){ handleStream(req,res,u); return; }
    serveStatic(req,res,u);
  }catch(e){console.error(e);if(!res.headersSent)writeJson(res,500,{ok:false,error:"Internal server error"});}
});

function handleStream(req,res,u){
  const role=u.searchParams.get("role"),id=(u.searchParams.get("roomId")||"").toUpperCase(),r=db.rooms[id];
  if(!r){res.writeHead(404);res.end("Room not found");return}
  const token=u.searchParams.get("token")||"",teamId=u.searchParams.get("teamId")||"";
  if(role==="organizer" && !token || role==="organizer" && !authControl(r,token)){res.writeHead(401);res.end("Unauthorized");return}
  if(role==="student" && !teamForSession(id,teamId,token)){res.writeHead(401);res.end("Unauthorized");return}
  res.writeHead(200,{"Content-Type":"text/event-stream; charset=utf-8","Cache-Control":"no-cache, no-transform","Connection":"keep-alive","Access-Control-Allow-Origin":"*","X-Accel-Buffering":"no"});
  res.write(`event: hello\ndata: ${JSON.stringify({serverTime:now()})}\n\n`);
  const c={res,role,teamId};roomClients(id).add(c);
  if(role==="organizer")sendSse(c,"organizer:joined",{roomId:id,onlineTeams:onlineCount(id),room:roomState(r),pendingEvent:publicEvent(pendingEvent(r))});
  else sendSse(c,"student:joined",{roomId:id,team:publicTeam(db.teams[teamId]),room:roomState(r)});
  sendSse(c,"room:state",roomState(r));
  const keep=setInterval(()=>{try{res.write(": keep-alive\n\n")}catch{}},15000);
  req.on("close",async()=>{clearInterval(keep);roomClients(id).delete(c);
    if(role==="organizer" && ![...roomClients(id)].some(x=>x.role==="organizer") && r.buzzerOpen){setClosed(r,"ORGANIZER_OFFLINE");await saveDb();broadcast(id,"buzzer:closed",{reason:"organizer-offline"});broadcast(id,"room:state",roomState(r));}
  });
}

setInterval(async()=>{
  for(const r of Object.values(db.rooms)){
    if(r.buzzerOpen&&r.buzzerExpiresAt&&now()>=r.buzzerExpiresAt){
      setClosed(r,"TIME_UP");await saveDb();broadcast(r.id,"buzzer:closed",{reason:"time-up"});broadcast(r.id,"room:state",roomState(r));
    }
  }
},250);

server.listen(PORT,HOST,()=>console.log(`TechASTRA Buzzer running on http://${HOST}:${PORT}`));
