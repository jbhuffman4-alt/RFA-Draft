const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ─── TEAM DATA ───────────────────────────────────────────────────────────────
const TEAM_DATA = {
  'Smash':{budget:187,spots:8,commissioner:true,players:[
    {name:'Trevor Lawrence',price:4},{name:'Josh Allen',price:40},{name:'Jaxon Smith-Njigba',price:4}
  ]},
  'Bryce':{budget:138,spots:6,commissioner:true,players:[
    {name:'Bijan Robinson',price:7}
  ]},
  'JJ':{budget:115,spots:8,commissioner:true,players:[
    {name:'Justin Herbert',price:37},{name:'Jakobi Meyers',price:2}
  ]},
  'Kevin':{budget:201,spots:14,commissioner:false,players:[
    {name:'Patrick Mahomes',price:33},{name:'Drake London',price:7}
  ]},
  'Jeremy':{budget:169,spots:9,commissioner:false,players:[
    {name:'Jahmyr Gibbs',price:6},{name:'Josh Jacobs',price:6},{name:'James Cook',price:2},
    {name:'Justin Jefferson',price:45},{name:'Garrett Wilson',price:1}
  ]},
  'Trevor':{budget:90,spots:6,commissioner:false,players:[
    {name:'Jaylen Warren',price:2},{name:'Kenneth Walker',price:5}
  ]},
  'Tate':{budget:216,spots:15,commissioner:false,players:[
    {name:'Dak Prescott',price:17},{name:'Lamar Jackson',price:24},{name:'Jonathan Taylor',price:60},
    {name:'Christian McCaffrey',price:47},{name:'CeeDee Lamb',price:30},{name:'Amon-Ra St Brown',price:7},
    {name:'George Pickens',price:2},{name:'Devonta Smith',price:3}
  ]},
  'Coop':{budget:186,spots:11,commissioner:false,players:[
    {name:'Jared Goff',price:2},{name:'Joe Burrow',price:27},{name:'Travis Etienne',price:24},
    {name:'Tyler Allgeier',price:2},{name:'Jamarr Chase',price:41},{name:'Chris Olave',price:2}
  ]},
  'Rando':{budget:88,spots:3,commissioner:false,players:[
    {name:'JK Dobbins',price:16}
  ]},
  'Zach':{budget:121,spots:8,commissioner:false,players:[
    {name:'Daniel Jones',price:2},{name:'Matthew Stafford',price:1},
    {name:'Jameson Williams',price:1},{name:'Jake Ferguson',price:2}
  ]}
};

const NFL_TEAMS = {
  'Trevor Lawrence':'JAC','Jared Goff':'DET','Dak Prescott':'DAL','Joe Burrow':'CIN',
  'Josh Allen':'BUF','Daniel Jones':'NYG','Matthew Stafford':'LAR','Justin Herbert':'LAC',
  'Lamar Jackson':'BAL','Patrick Mahomes':'KC','Travis Etienne':'JAC','Jahmyr Gibbs':'DET',
  'Bijan Robinson':'ATL','Tyler Allgeier':'ATL','JK Dobbins':'LAC','Jonathan Taylor':'IND',
  'Jaylen Warren':'PIT','Christian McCaffrey':'SF','Josh Jacobs':'GB','James Cook':'BUF',
  'Kenneth Walker':'SEA','CeeDee Lamb':'DAL','Amon-Ra St Brown':'DET','Jamarr Chase':'CIN',
  'George Pickens':'PIT','Chris Olave':'NO','Devonta Smith':'PHI','Drake London':'ATL',
  'Jameson Williams':'DET','Justin Jefferson':'MIN','Garrett Wilson':'NYJ',
  'Jaxon Smith-Njigba':'SEA','Jakobi Meyers':'LV','Jake Ferguson':'DAL'
};

const POS = {
  'Trevor Lawrence':'QB','Jared Goff':'QB','Dak Prescott':'QB','Joe Burrow':'QB',
  'Josh Allen':'QB','Daniel Jones':'QB','Matthew Stafford':'QB','Justin Herbert':'QB',
  'Lamar Jackson':'QB','Patrick Mahomes':'QB','Travis Etienne':'RB','Jahmyr Gibbs':'RB',
  'Bijan Robinson':'RB','Tyler Allgeier':'RB','JK Dobbins':'RB','Jonathan Taylor':'RB',
  'Jaylen Warren':'RB','Christian McCaffrey':'RB','Josh Jacobs':'RB','James Cook':'RB',
  'Kenneth Walker':'RB','CeeDee Lamb':'WR','Amon-Ra St Brown':'WR','Jamarr Chase':'WR',
  'George Pickens':'WR','Chris Olave':'WR','Devonta Smith':'WR','Drake London':'WR',
  'Jameson Williams':'WR','Justin Jefferson':'WR','Garrett Wilson':'WR',
  'Jaxon Smith-Njigba':'WR','Jakobi Meyers':'WR','Jake Ferguson':'TE'
};

// ─── SERVER STATE ─────────────────────────────────────────────────────────────
function buildAllPlayers() {
  const arr = [];
  Object.entries(TEAM_DATA).forEach(([t,d]) => d.players.forEach(p => {
    arr.push({name:p.name, basePrice:p.price, owner:t, pos:POS[p.name]||'WR', nfl:NFL_TEAMS[p.name]||'NFL'});
  }));
  return arr;
}

function freshState() {
  const budgets = {}, spots = {}, acquired = {};
  Object.keys(TEAM_DATA).forEach(t => {
    budgets[t] = TEAM_DATA[t].budget;
    spots[t] = TEAM_DATA[t].spots;
    acquired[t] = [];
  });
  return {
    started: false,
    paused: false,
    queue: buildAllPlayers(),
    nomOrder: [],
    nomIdx: 0,
    cur: null,
    curBid: 0,
    curBidder: null,
    bidHistory: [],
    phase: 'idle', // idle | bidding | raise | match | done
    timerSec: 30,
    timerMax: 30,
    completed: [],
    budgets,
    spots,
    acquired
  };
}

let G = freshState();
let draftHistory = [];
let timerInterval = null;
let connectedUsers = {}; // socketId -> teamName

function spotsLeft(t) { return G.spots[t] - G.acquired[t].length; }

function broadcast(event, data) { io.emit(event, data); }

function getPublicState() {
  return {
    started: G.started,
    paused: G.paused,
    queue: G.queue,
    nomOrder: G.nomOrder,
    nomIdx: G.nomIdx,
    cur: G.cur,
    curBid: G.curBid,
    curBidder: G.curBidder,
    bidHistory: G.bidHistory,
    phase: G.phase,
    timerSec: G.timerSec,
    timerMax: G.timerMax,
    completed: G.completed,
    budgets: G.budgets,
    spots: G.spots,
    acquired: G.acquired,
    connectedUsers: Object.values(connectedUsers)
  };
}

// ─── TIMER ───────────────────────────────────────────────────────────────────
function stopTimer() {
  if (timerInterval) { clearInterval(timerInterval); timerInterval = null; }
}

function startTimer(seconds, onEnd) {
  stopTimer();
  G.timerSec = seconds;
  G.timerMax = seconds;
  broadcast('timer_tick', { timerSec: G.timerSec, timerMax: G.timerMax, phase: G.phase });
  timerInterval = setInterval(() => {
    if (G.paused) return;
    G.timerSec--;
    broadcast('timer_tick', { timerSec: G.timerSec, timerMax: G.timerMax, phase: G.phase });
    if (G.timerSec <= 0) {
      stopTimer();
      onEnd();
    }
  }, 1000);
}

// ─── DRAFT FLOW ───────────────────────────────────────────────────────────────
function promptNomination() {
  stopTimer();
  G.cur = null; G.curBid = 0; G.curBidder = null; G.bidHistory = [];
  G.phase = 'idle';
  const nomTeam = G.nomOrder[G.nomIdx % G.nomOrder.length];
  broadcast('prompt_nomination', { nomTeam, queue: G.queue, state: getPublicState() });
}

function startAuction(player, nominatedBy) {
  G.cur = player;
  G.curBid = player.basePrice;
  G.curBidder = null;
  G.bidHistory = [];
  G.phase = 'bidding';
  G.queue = G.queue.filter(p => p.name !== player.name);
  broadcast('auction_start', { player, nominatedBy, state: getPublicState() });
  startTimer(30, endBidding);
}

function endBidding() {
  if (!G.cur) return;
  if (!G.curBidder) { finalizePlayer(G.cur, null, 0); return; }
  startRaisePhase();
}

function startRaisePhase() {
  G.phase = 'raise';
  broadcast('raise_phase', { curBid: G.curBid, curBidder: G.curBidder, state: getPublicState() });
  startTimer(15, () => {
    broadcast('raise_expired', {});
    proceedToOwnerDecision();
  });
}

function proceedToOwnerDecision() {
  G.phase = 'match';
  broadcast('match_phase', { cur: G.cur, curBid: G.curBid, curBidder: G.curBidder, state: getPublicState() });
  startTimer(30, () => {
    broadcast('match_expired', {});
    finalizePlayer(G.cur, G.curBidder, G.curBid);
  });
}

function finalizePlayer(player, winner, amount) {
  stopTimer();
  if (winner) {
    G.budgets[winner] -= amount;
    G.acquired[winner].push({ player: player.name, amount, pos: player.pos });
    G.completed.push({ player: player.name, winner, amount, signed: true });
  } else {
    G.completed.push({ player: player.name, winner: null, amount: 0, signed: false });
  }
  G.nomIdx++;
  G.phase = 'idle';
  if (G.queue.length === 0) {
    G.phase = 'done';
    // Save to history
    draftHistory.push({
      id: Date.now(),
      date: new Date().toLocaleString(),
      completed: [...G.completed],
      acquired: JSON.parse(JSON.stringify(G.acquired)),
      budgets: { ...G.budgets }
    });
    broadcast('draft_complete', { state: getPublicState(), history: draftHistory });
    return;
  }
  broadcast('player_finalized', { player, winner, amount, state: getPublicState() });
  promptNomination();
}

// ─── SOCKET EVENTS ────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  socket.on('login', (teamName) => {
    connectedUsers[socket.id] = teamName;
    socket.emit('login_ok', {
      team: teamName,
      teamData: TEAM_DATA[teamName],
      state: getPublicState(),
      history: draftHistory
    });
    broadcast('users_update', Object.values(connectedUsers));
    console.log(`${teamName} logged in`);
  });

  socket.on('start_draft', () => {
    if (!G.started) {
      const teams = Object.keys(TEAM_DATA);
      G.nomOrder = teams.sort(() => Math.random() - 0.5);
      G.started = true;
      broadcast('draft_started', { state: getPublicState() });
      promptNomination();
    }
  });

  socket.on('nominate', (playerName) => {
    const team = connectedUsers[socket.id];
    const nomTeam = G.nomOrder[G.nomIdx % G.nomOrder.length];
    if (team !== nomTeam) return;
    const player = G.queue.find(p => p.name === playerName);
    if (!player) return;
    startAuction(player, team);
  });

  socket.on('bid', (amount) => {
    if (G.phase !== 'bidding') return;
    const team = connectedUsers[socket.id];
    if (!team || !G.cur) return;
    if (G.cur.owner === team) return;
    if (spotsLeft(team) <= 0) { socket.emit('bid_error', 'No roster spots left!'); return; }
    if (isNaN(amount) || amount < G.cur.basePrice) { socket.emit('bid_error', `Min bid is $${G.cur.basePrice}`); return; }
    if (G.curBidder && amount <= G.curBid) { socket.emit('bid_error', `Must beat $${G.curBid}`); return; }
    if (amount > G.budgets[team]) { socket.emit('bid_error', `Not enough budget ($${G.budgets[team]} left)`); return; }
    G.curBid = amount;
    G.curBidder = team;
    G.bidHistory.unshift({ team, amount });
    // Add 5 seconds
    G.timerSec = Math.min(G.timerSec + 5, 60);
    G.timerMax = Math.max(G.timerMax, G.timerSec);
    broadcast('bid_placed', { team, amount, timerSec: G.timerSec, timerMax: G.timerMax, bidHistory: G.bidHistory, budgets: G.budgets });
  });

  socket.on('raise_bid', (amount) => {
    if (G.phase !== 'raise') return;
    const team = connectedUsers[socket.id];
    if (team !== G.curBidder) return;
    if (isNaN(amount) || amount <= G.curBid) { socket.emit('bid_error', `Must be higher than $${G.curBid}`); return; }
    if (amount > G.budgets[team]) { socket.emit('bid_error', `Not enough budget`); return; }
    stopTimer();
    G.curBid = amount;
    G.bidHistory.unshift({ team, amount, raise: true });
    broadcast('bid_placed', { team, amount, timerSec: G.timerSec, timerMax: G.timerMax, bidHistory: G.bidHistory, budgets: G.budgets });
    proceedToOwnerDecision();
  });

  socket.on('raise_skip', () => {
    if (G.phase !== 'raise') return;
    const team = connectedUsers[socket.id];
    if (team !== G.curBidder) return;
    stopTimer();
    proceedToOwnerDecision();
  });

  socket.on('owner_decision', (match) => {
    if (G.phase !== 'match') return;
    const team = connectedUsers[socket.id];
    if (team !== G.cur.owner) return;
    stopTimer();
    finalizePlayer(G.cur, match ? G.cur.owner : G.curBidder, G.curBid);
  });

  // Commissioner controls
  socket.on('comm_pause', () => { G.paused = true; broadcast('paused', {}); });
  socket.on('comm_resume', () => { G.paused = false; broadcast('resumed', {}); });
  socket.on('comm_skip', () => {
    stopTimer();
    if (G.cur) G.queue = G.queue.filter(p => p.name !== G.cur.name);
    G.nomIdx++;
    broadcast('player_skipped', { state: getPublicState() });
    promptNomination();
  });
  socket.on('comm_override', ({ amount, team }) => {
    if (!TEAM_DATA[team]) return;
    G.curBid = amount; G.curBidder = team;
    G.bidHistory.unshift({ team, amount });
    broadcast('bid_placed', { team, amount, timerSec: G.timerSec, timerMax: G.timerMax, bidHistory: G.bidHistory, budgets: G.budgets });
  });

  socket.on('reset_draft', () => {
    const team = connectedUsers[socket.id];
    if (!TEAM_DATA[team]?.commissioner) return;
    stopTimer();
    G = freshState();
    broadcast('draft_reset', { state: getPublicState(), history: draftHistory });
  });

  socket.on('disconnect', () => {
    const team = connectedUsers[socket.id];
    delete connectedUsers[socket.id];
    broadcast('users_update', Object.values(connectedUsers));
    console.log(`${team || socket.id} disconnected`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`RFA Draft server running on port ${PORT}`));
