// Comet Miner multiplayer over Firebase Realtime Database.
// Gives the game the same "room" the Claude page has: peers(), onPeers(), presence(), emit(), on().
(function(){
  const SDK='https://www.gstatic.com/firebasejs/10.12.2/';
  const load=src=>new Promise((ok,bad)=>{const s=document.createElement('script');s.src=src;s.onload=ok;s.onerror=bad;document.head.appendChild(s)});
  let made=null;
  window.cometFirebaseRoom=function(){
    if(!window.FIREBASE_CONFIG)return Promise.resolve(null);
    if(!made)made=(async()=>{
      await load(SDK+'firebase-app-compat.js');
      await Promise.all([load(SDK+'firebase-auth-compat.js'),load(SDK+'firebase-database-compat.js')]);
      const app=firebase.apps.length?firebase.app():firebase.initializeApp(window.FIREBASE_CONFIG);
      const cred=await app.auth().signInAnonymously();
      const uid=cred.user.uid,db=app.database(),base=db.ref('comet');
      const meRef=base.child('peers').push(),me=meRef.key;
      meRef.onDisconnect().remove();
      let list=[{peer:me,sameTab:true,isMe:true,presence:{}}];
      const peerCbs=[],evRefs=[];
      base.child('peers').on('value',snap=>{
        const v=snap.val()||{};list=[{peer:me,sameTab:true,isMe:true,presence:{}}];
        for(const k in v)if(k!==me)list.push({peer:k,sameTab:false,isMe:v[k].uid===uid,presence:v[k].p||{}});
        for(const cb of peerCbs)cb({peers:list});
      });
      let last=0,pending=null,timer=null;
      const send=()=>{timer=null;last=Date.now();const p=pending;pending=null;
        return p&&p.mp?meRef.set({uid,p,t:firebase.database.ServerValue.TIMESTAMP}):meRef.remove()};
      return {
        peers:()=>list,
        onPeers(cb){peerCbs.push(cb);cb({peers:list});return()=>{}},
        presence(p){pending=p;if(!p||!p.mp){clearTimeout(timer);return send()}
          if(!timer)timer=setTimeout(send,Math.max(0,66-(Date.now()-last)));return Promise.resolve()},
        async emit(topic,data){const r=base.child('events/'+topic).push();await r.set({from:me,uid,data,t:firebase.database.ServerValue.TIMESTAMP});setTimeout(()=>r.remove().catch(()=>{}),5000)},
        on(topic,cb){
          const q=base.child('events/'+topic).orderByChild('t').startAt(Date.now()-3000);
          q.on('child_added',s=>{const e=s.val();if(!e||e.from===me)return;cb({peer:e.from,sameTab:false,isMe:e.uid===uid,data:e.data})});
          evRefs.push(q);return()=>q.off();
        }
      };
    })().catch(e=>{console.warn('multiplayer:',e);made=null;return null});
    return made;
  };
})();
