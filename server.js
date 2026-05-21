const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  pingTimeout: 60000,
  pingInterval: 25000,
  connectTimeout: 45000,
  transports: ['websocket', 'polling'],
});

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.redirect('/host.html');
});

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    phase: state.phase,
    players: Object.keys(state.players).length,
  });
});

const PORT = process.env.PORT || 3000;

function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) return iface.address;
    }
  }
  return 'localhost';
}
const LOCAL_IP = getLocalIP();

// ─── PREGUNTAS ────────────────────────────────────────────────────────────────
const QUESTIONS = [
  {
    question: "¿Qué describe mejor el networking?",
    options: ["Agregar personas en redes sociales", "Acumular tarjetas de presentación", "Construir vínculos genuinos de confianza mutua", "Seguir empresas en LinkedIn"],
    correct: 2,
    explanation: "El networking va más allá de contactos superficiales: se trata de construir relaciones auténticas basadas en confianza y valor mutuo."
  },
  {
    question: "¿Cuánto debe durar aproximadamente un elevator pitch?",
    options: ["1 minuto", "45 segundos", "30 segundos", "2 minutos"],
    correct: 2,
    explanation: "Un elevator pitch dura unos 30 segundos: breve, claro y memorable. Más tiempo lo hace perder impacto."
  },
  {
    question: "¿Qué significa literalmente la palabra 'Networking'?",
    options: ["Red de amigos", "Trabajo en equipo", "Trabajar en red", "Conexión digital"],
    correct: 2,
    explanation: "'Net' significa red y 'working' significa trabajar. Networking es literalmente trabajar en red para construir relaciones profesionales."
  },
  {
    question: "¿Cuál de estos NO es un tipo de networking mencionado en la exposición?",
    options: ["Networking presencial", "Networking digital", "Networking viral", "Networking de mentoría"],
    correct: 2,
    explanation: "El 'networking viral' no existe como categoría. Los 5 tipos reales son: presencial, online, estratégico, operacional y de mentoría."
  },
  {
    question: "¿Cuál es la ventaja PRINCIPAL del networking online frente al presencial?",
    options: ["Es más barato", "No requiere preparación", "No tiene límites geográficos", "Genera más confianza"],
    correct: 2,
    explanation: "El networking online permite ampliar contactos sin limitaciones geográficas, conectando con personas en cualquier parte del mundo."
  },
  {
    question: "¿Cuál de estos es un beneficio del networking mencionado en la exposición?",
    options: ["Garantía de conseguir empleo", "Acceso a mentores y experiencias", "Eliminar la competencia", "Reducir costos empresariales"],
    correct: 1,
    explanation: "El networking brinda acceso a mentores con experiencia que comparten consejos y orientación. No garantiza empleo, pero abre puertas."
  },
  {
    question: "¿Cuántos tipos de networking se presentaron en total en la exposición?",
    options: ["3 tipos", "4 tipos", "6 tipos", "5 tipos"],
    correct: 3,
    explanation: "Se presentaron 5 tipos: presencial, online, estratégico, operacional y de mentoría. Cada uno con propósito diferente."
  },
  {
    question: "¿Qué tipo de networking se centra en crear conexiones para alcanzar objetivos ESPECÍFICOS y planeados?",
    options: ["Networking presencial", "Networking operacional", "Networking estratégico", "Networking de mentoría"],
    correct: 2,
    explanation: "El networking estratégico no es casualidad: se planea para conectar con personas que aporten a un objetivo concreto."
  },
  {
    question: "¿Qué tipo de networking ocurre DENTRO de una organización entre colegas y equipos internos?",
    options: ["Estratégico", "Presencial", "Digital", "Operacional"],
    correct: 3,
    explanation: "El networking operacional es interno. El estratégico es externo con metas específicas. No confundirlos."
  },
  {
    question: "¿Qué práctica impulsa oportunidades en sostenibilidad y desarrollo social?",
    options: ["Publicar contenido en redes sociales", "Crear alianzas entre empresas, ONGs y gobiernos", "Asistir a cursos de liderazgo", "Tener un perfil actualizado en LinkedIn"],
    correct: 1,
    explanation: "Las alianzas entre empresas, ONGs y gobiernos generan impacto real en sostenibilidad y desarrollo social, más allá de la visibilidad individual en redes."
  },
  {
    question: "¿Qué herramienta VERBAL es la más importante para presentarse en un evento de networking?",
    options: ["Tarjeta de presentación", "Portafolio impreso", "Elevator pitch", "Código QR de LinkedIn"],
    correct: 2,
    explanation: "El elevator pitch es el discurso de 30 segundos donde explicas quién eres y qué valor ofreces. Es la herramienta verbal clave del networking."
  },
  {
    question: "Según la exposición, ¿cuál es la PRIMERA acción antes de ir a un evento de networking?",
    options: ["Llevar tarjetas de presentación", "Preparar el elevator pitch", "Actualizar LinkedIn", "Investigar quiénes asistirán"],
    correct: 3,
    explanation: "Lo primero: investigar quiénes asistirán y definir con quién conectar. El elevator pitch viene después. No al revés."
  }
];

// ─── ESTADO DEL JUEGO ─────────────────────────────────────────────────────────
let state = {
  players: {},
  currentQuestion: -1,
  gameStarted: false,
  questionActive: false,
  timer: null,
  timeLeft: 10,
  phase: 'lobby',
};

const disconnectTimers = new Map();
let emptyGameTimer = null;
const DISCONNECT_GRACE_MS = 20000;
const EMPTY_GAME_RESET_MS = 45000;

function newRejoinKey() {
  return crypto.randomBytes(8).toString('hex');
}

function cancelEmptyGameReset() {
  if (emptyGameTimer) {
    clearTimeout(emptyGameTimer);
    emptyGameTimer = null;
  }
}

function scheduleEmptyGameReset() {
  cancelEmptyGameReset();
  if (!state.gameStarted || hasActivePlayers()) return;
  emptyGameTimer = setTimeout(() => {
    emptyGameTimer = null;
    if (state.gameStarted && !hasActivePlayers()) startFreshSession(true);
  }, EMPTY_GAME_RESET_MS);
}

function clearDisconnectTimers() {
  disconnectTimers.forEach((t) => clearTimeout(t));
  disconnectTimers.clear();
}

function resetState() {
  if (state.timer) clearInterval(state.timer);
  clearDisconnectTimers();
  cancelEmptyGameReset();
  state = { players: {}, currentQuestion: -1, gameStarted: false, questionActive: false, timer: null, timeLeft: 10, phase: 'lobby' };
}

function findPlayerByRejoinKey(rejoinKey) {
  if (!rejoinKey) return null;
  for (const [id, p] of Object.entries(state.players)) {
    if (p.rejoinKey === rejoinKey) return [id, p];
  }
  return null;
}

function migratePlayerSocket(oldId, socket, player) {
  if (disconnectTimers.has(oldId)) {
    clearTimeout(disconnectTimers.get(oldId));
    disconnectTimers.delete(oldId);
  }
  delete state.players[oldId];
  player.disconnected = false;
  state.players[socket.id] = player;
  cancelEmptyGameReset();
}

function schedulePlayerRemoval(socketId) {
  const player = state.players[socketId];
  if (!player || disconnectTimers.has(socketId)) return;
  player.disconnected = true;
  io.to('host').emit('playerList', getLeaderboard());
  disconnectTimers.set(socketId, setTimeout(() => {
    disconnectTimers.delete(socketId);
    if (state.players[socketId]) {
      delete state.players[socketId];
      io.to('host').emit('playerList', getLeaderboard());
      if (state.gameStarted && !hasActivePlayers()) scheduleEmptyGameReset();
    }
  }, DISCONNECT_GRACE_MS));
}

function syncPlayerState(socket, player) {
  socket.emit('joinSuccess', { name: player.name, rejoinKey: player.rejoinKey, score: player.score, lives: player.lives, eliminated: player.eliminated });
  if (!state.gameStarted) return;

  if (player.eliminated) {
    socket.emit('playerFeedback', { correct: false, eliminated: true });
    return;
  }

  if (state.phase === 'gameover') {
    socket.emit('gameOver', { leaderboard: getLeaderboard() });
    return;
  }

  const q = QUESTIONS[state.currentQuestion];
  if (state.phase === 'question' || state.phase === 'reveal') {
    socket.emit('question', {
      index: state.currentQuestion,
      total: QUESTIONS.length,
      question: q.question,
      options: q.options,
    });
    if (state.phase === 'question') socket.emit('timerUpdate', state.timeLeft);
    if (state.phase === 'reveal') {
      const isLast = state.currentQuestion >= QUESTIONS.length - 1;
      socket.emit('reveal', { correct: q.correct, explanation: q.explanation, leaderboard: getLeaderboard(), isLast });
    }
  }
}

function hasActivePlayers() {
  return Object.keys(state.players).length > 0;
}

/** Partida terminada o abandonada (sin jugadores) — lista para una nueva ronda */
function shouldStartFreshSession() {
  if (state.phase === 'gameover') return true;
  if (state.gameStarted && !hasActivePlayers()) return true;
  return false;
}

function startFreshSession(notifyClients = true) {
  resetState();
  if (notifyClients) io.emit('gameReset');
}

function getLeaderboard() {
  return Object.entries(state.players)
    .map(([id, p]) => ({
      id,
      name: p.name,
      score: p.score,
      lives: p.lives,
      eliminated: p.eliminated,
      answered: p.answered,
      disconnected: !!p.disconnected,
    }))
    .sort((a, b) => b.score - a.score);
}

function getActivePlayers() {
  return Object.values(state.players).filter(p => !p.eliminated);
}

function getAnsweredCount() {
  const active = getActivePlayers();
  return { answered: active.filter(p => p.answered).length, total: active.length };
}

function sendQuestion() {
  const q = QUESTIONS[state.currentQuestion];
  state.questionActive = true;
  state.timeLeft = 10;
  state.phase = 'question';
  Object.values(state.players).forEach(p => { p.answered = false; });

  const payload = { index: state.currentQuestion, total: QUESTIONS.length, question: q.question, options: q.options };
  io.emit('question', payload);

  if (state.timer) clearInterval(state.timer);
  state.timer = setInterval(() => {
    state.timeLeft--;
    io.emit('timerUpdate', state.timeLeft);
    if (state.timeLeft <= 0) {
      clearInterval(state.timer);
      state.questionActive = false;
      handleTimeUp();
    }
  }, 1000);
}

function handleTimeUp() {
  Object.entries(state.players).forEach(([id, player]) => {
    if (!player.answered && !player.eliminated) {
      player.lives -= 1;
      if (player.lives <= 0) {
        player.eliminated = true;
        io.to(id).emit('playerFeedback', { correct: false, eliminated: true, timeUp: true });
      } else {
        io.to(id).emit('playerFeedback', { correct: false, lives: player.lives, timeUp: true });
      }
    }
  });
  revealAnswer();
}

function revealAnswer() {
  state.phase = 'reveal';
  const q = QUESTIONS[state.currentQuestion];
  const isLast = state.currentQuestion >= QUESTIONS.length - 1;
  io.emit('reveal', { correct: q.correct, explanation: q.explanation, leaderboard: getLeaderboard(), isLast });
}

function checkAllAnswered() {
  const active = getActivePlayers();
  if (active.length === 0) return;
  if (active.every(p => p.answered)) {
    if (state.timer) clearInterval(state.timer);
    state.questionActive = false;
    revealAnswer();
  }
}

// ─── SOCKET.IO ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  socket.on('hostConnect', () => {
    socket.join('host');
    if (shouldStartFreshSession()) startFreshSession(false);
    socket.emit('hostInit', {
      ip: LOCAL_IP,
      port: PORT,
      players: getLeaderboard(),
      totalQuestions: QUESTIONS.length,
      phase: state.phase,
      gameStarted: state.gameStarted,
      currentQuestion: state.currentQuestion,
    });
  });

  socket.on('playerJoin', ({ name, rejoinKey }) => {
    const trimmed = name.trim().slice(0, 20);
    if (!trimmed) return;

    const existing = findPlayerByRejoinKey(rejoinKey);
    if (existing) {
      const [oldId, player] = existing;
      migratePlayerSocket(oldId, socket, player);
      syncPlayerState(socket, player);
      io.to('host').emit('playerList', getLeaderboard());
      return;
    }

    if (state.gameStarted) {
      socket.emit('joinError', 'El juego ya comenzó. Espera la próxima ronda.');
      return;
    }

    const nameTaken = Object.values(state.players).some(
      (p) => !p.disconnected && p.name.toLowerCase() === trimmed.toLowerCase()
    );
    if (nameTaken) {
      socket.emit('joinError', 'Ese nombre ya está en uso. Elige otro.');
      return;
    }

    const key = newRejoinKey();
    state.players[socket.id] = {
      name: trimmed,
      score: 0,
      lives: 3,
      answered: false,
      eliminated: false,
      disconnected: false,
      rejoinKey: key,
    };
    cancelEmptyGameReset();
    socket.emit('joinSuccess', { name: trimmed, rejoinKey: key });
    io.to('host').emit('playerList', getLeaderboard());
  });

  socket.on('startGame', () => {
    const connected = Object.values(state.players).filter((p) => !p.disconnected);
    if (connected.length === 0) return;
    state.gameStarted = true;
    state.currentQuestion = 0;
    io.emit('gameStarting');
    setTimeout(() => sendQuestion(), 3500);
  });

  socket.on('answer', ({ questionIndex, answerIndex }) => {
    const player = state.players[socket.id];
    if (!player || player.answered || player.eliminated) return;
    if (questionIndex !== state.currentQuestion || !state.questionActive) return;

    player.answered = true;
    const q = QUESTIONS[state.currentQuestion];
    const isCorrect = answerIndex === q.correct;
    const timeBonus = Math.round((state.timeLeft / 10) * 500);

    if (isCorrect) {
      const points = 1000 + timeBonus;
      player.score += points;
      socket.emit('playerFeedback', { correct: true, points, lives: player.lives });
    } else {
      player.lives -= 1;
      if (player.lives <= 0) {
        player.eliminated = true;
        socket.emit('playerFeedback', { correct: false, eliminated: true });
      } else {
        socket.emit('playerFeedback', { correct: false, lives: player.lives });
      }
    }

    io.to('host').emit('playerList', getLeaderboard());
    io.to('host').emit('answeredUpdate', getAnsweredCount());
    checkAllAnswered();
  });

  socket.on('nextQuestion', () => {
    state.currentQuestion++;
    if (state.currentQuestion >= QUESTIONS.length) {
      state.phase = 'gameover';
      io.emit('gameOver', { leaderboard: getLeaderboard() });
    } else {
      sendQuestion();
    }
  });

  socket.on('resetGame', () => {
    startFreshSession(true);
  });

  socket.on('disconnect', () => {
    if (state.players[socket.id]) {
      if (!state.gameStarted) {
        if (disconnectTimers.has(socket.id)) clearTimeout(disconnectTimers.get(socket.id));
        disconnectTimers.delete(socket.id);
        delete state.players[socket.id];
        io.to('host').emit('playerList', getLeaderboard());
      } else {
        schedulePlayerRemoval(socket.id);
      }
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n╔══════════════════════════════════════════╗');
  console.log('║        🎮  NETWORKING TRIVIA             ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║  📺  Host (proyectar):                   ║`);
  console.log(`║  http://localhost:${PORT}/host.html         ║`);
  console.log(`║                                          ║`);
  console.log(`║  📱  Jugadores (celular):                ║`);
  console.log(`║  http://${LOCAL_IP}:${PORT}/player.html  ║`);
  console.log('╚══════════════════════════════════════════╝\n');
});
