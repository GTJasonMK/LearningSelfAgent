const { app, BrowserWindow, Menu, Tray, nativeImage, ipcMain, screen } = require("electron");
const { spawn } = require("child_process");
const http = require("http");
const net = require("net");
const path = require("path");

let tray = null;
let petWindow = null;
let panelWindow = null;
let backendProcess = null;
let backendSpawned = false;
let lastHitState = null;
let ignoreMouseEvents = true;
let petDragging = false;
let petDragOffset = null;
let petDragTimer = null;
let petHitTestTimer = null;
let petRecoveryTimer = null;
let petWindowCreating = false;
let quitting = false;
const BACKEND_DEFAULT_PORT = 8123;

function parseBackendPort(raw) {
  const value = Number.parseInt(String(raw || "").trim(), 10);
  if (Number.isInteger(value) && value >= 1 && value <= 65535) return value;
  return null;
}

function isPortAvailable(port) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", () => resolve(false));
    server.once("listening", () => {
      server.close(() => resolve(true));
    });
    server.listen(port, "127.0.0.1");
  });
}

function getEphemeralPort() {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", () => resolve(BACKEND_DEFAULT_PORT));
    server.once("listening", () => {
      const address = server.address();
      const port = (address && typeof address === "object") ? Number(address.port) : BACKEND_DEFAULT_PORT;
      server.close(() => resolve(port));
    });
    server.listen(0, "127.0.0.1");
  });
}

async function resolveAvailableBackendPort(preferredPort) {
  if (await isPortAvailable(preferredPort)) return preferredPort;
  const scanStart = Math.max(1024, Number(preferredPort) + 1);
  const scanEnd = Math.min(65535, scanStart + 200);
  for (let candidate = scanStart; candidate <= scanEnd; candidate += 1) {
    if (await isPortAvailable(candidate)) return candidate;
  }
  return getEphemeralPort();
}

let backendPort = parseBackendPort(process.env.LSA_BACKEND_PORT) || BACKEND_DEFAULT_PORT;
process.env.LSA_BACKEND_PORT = String(backendPort);
const PET_HIT_TEST_INTERVAL = 33;
const PET_DRAG_INTERVAL = 16;
const PET_RECOVERY_INTERVAL = 2000;

// 桌宠窗口尺寸（DIP）
// 说明：用户要求“气泡/计划/输入框全部放回同一个渲染窗口”，因此需要一次性预留足够空间，
// 避免内部组件因窗口边界被裁剪；透明区域通过命中检测继续保持穿透。
const PET_WINDOW_SIZE = { width: 720, height: 480 };

function startBackend() {
  if (backendProcess) return;
  // 允许外部脚本/用户自行托管后端（例如 scripts/start.py 已经启动了 uvicorn）
  const skip = String(process.env.LSA_SKIP_BACKEND || "").toLowerCase();
  if (skip === "1" || skip === "true" || skip === "yes") {
    return;
  }

  // 如果端口上已经有健康的后端在跑，就不要再启动第二个（避免 10048 端口占用）
  checkBackendReady(800).then(async (state) => {
    // ready: 后端已就绪；occupied: 端口被占用但非本后端/尚未就绪；free: 可启动
    if (state === "ready") return;
    if (state === "occupied") {
      const nextPort = await resolveAvailableBackendPort(backendPort);
      if (Number(nextPort) !== Number(backendPort)) {
        console.warn("[backend] 端口 " + backendPort + " 已占用，自动切换到 " + nextPort);
        backendPort = Number(nextPort);
        process.env.LSA_BACKEND_PORT = String(backendPort);
      }
    }
    spawnBackendProcess();
  });
}

function checkBackendReady(timeoutMs) {
  return new Promise((resolve) => {
    const req = http.get(
      {
        hostname: "127.0.0.1",
        port: backendPort,
        path: "/api/health",
        timeout: timeoutMs
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => {
          data += String(chunk || "");
        });
        res.on("end", () => {
          if (res.statusCode === 200 && data.includes("\"ok\"")) {
            return resolve("ready");
          }
          // 有响应但不是健康检查：视为端口被占用（继续启动会直接报 10048）
          resolve("occupied");
        });
      }
    );
    req.on("timeout", () => {
      try { req.destroy(); } catch (e) {}
      resolve("free");
    });
    req.on("error", () => resolve("free"));
  });
}

function spawnBackendProcess() {
  if (backendProcess) return;
  const repoRoot = path.join(__dirname, "..", "..", "..");
  
  // Try to find python in venv first.
  // 约定：Windows 使用 `.venv-win`，WSL/Linux 使用 `.venv`，避免共用 `.venv/pyvenv.cfg` 互相污染。
  let python = "python";
  const fs = require("fs");

  function readVenvHome(venvDir) {
    try {
      const cfg = path.join(venvDir, "pyvenv.cfg");
      if (!fs.existsSync(cfg)) return null;
      const text = String(fs.readFileSync(cfg, "utf8") || "");
      const lines = text.split(/\r?\n/);
      for (const line of lines) {
        const trimmed = String(line || "").trim();
        if (!trimmed.startsWith("home")) continue;
        const idx = trimmed.indexOf("=");
        if (idx === -1) continue;
        return trimmed.slice(idx + 1).trim();
      }
    } catch (e) {}
    return null;
  }

  function venvMatchesPlatform(venvDir) {
    const home = readVenvHome(venvDir);
    if (!home) return true;
    if (process.platform === "win32") return !String(home).startsWith("/");
    return !String(home).includes(":") && !String(home).includes("\\");
  }

  // 允许覆盖 venv 目录（相对路径以 repoRoot 为基准）
  const venvOverrideRaw = process.env.LSA_VENV_DIR;
  const venvOverride = venvOverrideRaw ? String(venvOverrideRaw).trim().replace(/^["']+/, "").replace(/["']+$/, "").trim() : "";
  const venvDirs = [];
  if (venvOverride) {
    venvDirs.push(path.isAbsolute(venvOverride) ? venvOverride : path.join(repoRoot, venvOverride));
  } else if (process.platform === "win32") {
    venvDirs.push(path.join(repoRoot, ".venv-win"));
    venvDirs.push(path.join(repoRoot, ".venv"));
  } else {
    venvDirs.push(path.join(repoRoot, ".venv"));
    venvDirs.push(path.join(repoRoot, ".venv-win"));
  }

  for (const venvDir of venvDirs) {
    try {
      if (!venvMatchesPlatform(venvDir)) continue;
      const cand = process.platform === "win32"
        ? path.join(venvDir, "Scripts", "python.exe")
        : path.join(venvDir, "bin", "python");
      if (fs.existsSync(cand)) {
        python = cand;
        break;
      }
    } catch (e) {}
  }

  // 允许用户覆盖 Python 路径，但要做去引号/存在性校验，避免出现 '"...python.exe' 或 /usr/bin\python.exe 这类错误
  const rawOverride = process.env.LSA_PYTHON;
  if (rawOverride) {
    const override = String(rawOverride).trim().replace(/^["']+/, "").replace(/["']+$/, "").trim();
    const looksLikePath = (
      override.includes("/") ||
      override.includes("\\") ||
      override.includes(":") ||
      override.toLowerCase().endsWith(".exe")
    );

    if (!looksLikePath || fs.existsSync(override)) {
      python = override;
    } else {
      console.warn(`[backend] 忽略无效的 LSA_PYTHON=${JSON.stringify(rawOverride)}（文件不存在）`);
    }
  }

  const args = [
    "-m",
    "uvicorn",
    "backend.src.main:app",
    "--host",
    "127.0.0.1",
    "--port",
    String(backendPort)
  ];

  // 启动后端服务，Electron 负责托管生命周期
  backendProcess = spawn(python, args, {
    cwd: repoRoot,
    env: { ...process.env, PYTHONUNBUFFERED: "1" },
    stdio: "inherit"
  });
  backendSpawned = true;

  backendProcess.on("exit", () => {
    backendProcess = null;
    backendSpawned = false;
  });

  backendProcess.on("error", () => {
    backendProcess = null;
    backendSpawned = false;
  });
}

function stopBackend() {
  if (!backendProcess) return;
  if (!backendSpawned) return;
  backendProcess.kill();
  backendProcess = null;
  backendSpawned = false;
}

function stopRunningTasksOnExit(timeoutMs = 800) {
  return new Promise((resolve) => {
    const payload = "{}";
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port: backendPort,
        path: "/api/maintenance/stop-running",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload)
        },
        timeout: timeoutMs
      },
      (res) => {
        // 不关心响应体，只要请求发出即可；读完避免 socket 悬挂
        res.on("data", () => {});
        res.on("end", () => resolve());
      }
    );
    req.on("timeout", () => {
      try { req.destroy(); } catch (e) {}
      resolve();
    });
    req.on("error", () => resolve());
    try {
      req.write(payload);
    } catch (e) {}
    try {
      req.end();
    } catch (e) {
      resolve();
    }
  });
}

function createPetWindow() {
  if (petWindow && !petWindow.isDestroyed()) return;
  if (petWindowCreating) return;
  petWindowCreating = true;
  petWindow = new BrowserWindow({
    width: PET_WINDOW_SIZE.width,
    height: PET_WINDOW_SIZE.height,
    transparent: true,
    frame: false,
    resizable: false,
    alwaysOnTop: true,
    skipTaskbar: true,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });

  petWindow.loadFile(path.join(__dirname, "..", "renderer", "pet.html"));

  // 初始启用穿透并转发鼠标事件，确保渲染层可做命中检测
  petWindow.setIgnoreMouseEvents(true, { forward: true });
  startPetHitTestLoop();
  petWindow.webContents.once("did-finish-load", () => {
    petWindowCreating = false;
  });
  petWindow.once("ready-to-show", () => {
    petWindowCreating = false;
  });
  petWindow.webContents.on("render-process-gone", (_event, details) => {
    if (quitting) return;
    try { petWindow.destroy(); } catch (e) {}
    petWindow = null;
    petWindowCreating = false;
    // 渲染进程异常时自动恢复桌宠窗口
    setTimeout(() => restorePetWindow(), 300);
  });
  petWindow.on("unresponsive", () => {
    if (quitting) return;
    try { petWindow.destroy(); } catch (e) {}
    petWindow = null;
    petWindowCreating = false;
    setTimeout(() => restorePetWindow(), 300);
  });
  petWindow.on("closed", () => {
    stopPetHitTestLoop();
    stopPetDragLoop();
    petWindow = null;
    petWindowCreating = false;
  });
}

function setPetIgnoreMouseEvents(ignore) {
  if (!petWindow || petWindow.isDestroyed()) return;
  if (ignoreMouseEvents === ignore) return;
  ignoreMouseEvents = ignore;
  if (ignore) {
    // 保持穿透但允许渲染层收到鼠标事件做命中检测
    petWindow.setIgnoreMouseEvents(true, { forward: true });
  } else {
    petWindow.setIgnoreMouseEvents(false);
  }
}

ipcMain.on("pet-hit-test-result", (event, isHit) => {
  if (!petWindow) return;
  if (petDragging) {
    setPetIgnoreMouseEvents(false);
    return;
  }
  if (isHit === lastHitState) return;
  lastHitState = isHit;
  setPetIgnoreMouseEvents(!isHit);
});

ipcMain.on("pet-drag-start", () => {
  if (!petWindow) return;
  const point = screen.getCursorScreenPoint();
  const bounds = petWindow.getBounds();
  petDragOffset = { x: point.x - bounds.x, y: point.y - bounds.y };
  petDragging = true;
  setPetIgnoreMouseEvents(false);
  startPetDragLoop();
});

ipcMain.on("pet-drag-end", () => {
  stopPetDragLoop();
  petDragging = false;
  petDragOffset = null;
  lastHitState = null;
  // 拖拽结束后优先恢复穿透，避免透明区域继续遮挡
  setPetIgnoreMouseEvents(true);
});

function startPetDragLoop() {
  stopPetDragLoop();
  petDragTimer = setInterval(() => {
    if (!petWindow || petWindow.isDestroyed()) return;
    if (!petDragging || !petDragOffset) return;
    const point = screen.getCursorScreenPoint();
    const nextX = Math.round(point.x - petDragOffset.x);
    const nextY = Math.round(point.y - petDragOffset.y);
    const [curX, curY] = petWindow.getPosition();
    if (curX === nextX && curY === nextY) return;
    petWindow.setPosition(nextX, nextY, false);
  }, PET_DRAG_INTERVAL);
}

function stopPetDragLoop() {
  if (!petDragTimer) return;
  clearInterval(petDragTimer);
  petDragTimer = null;
}

// 说明：桌宠窗口保持固定尺寸（不再根据气泡/输入框自动缩放），避免内容变化导致窗口位置或布局抖动。

function startPetHitTestLoop() {
  stopPetHitTestLoop();
  petHitTestTimer = setInterval(() => {
    if (!petWindow || petWindow.isDestroyed()) return;
    if (petDragging) return;
    const point = screen.getCursorScreenPoint();
    const bounds = petWindow.getContentBounds();
    const inside = point.x >= bounds.x
      && point.x <= bounds.x + bounds.width
      && point.y >= bounds.y
      && point.y <= bounds.y + bounds.height;
    if (!inside) {
      // 仅在穿透模式下强制恢复穿透，避免按住/即将拖拽时被轮询打断事件链
      if (ignoreMouseEvents) {
        lastHitState = false;
        setPetIgnoreMouseEvents(true);
      }
      return;
    }
    try {
      petWindow.webContents.send("pet-hit-test-point", { cursor: point, bounds });
    } catch (e) {
      // webContents 可能在销毁或尚未就绪，忽略即可
    }
  }, PET_HIT_TEST_INTERVAL);
}

function stopPetHitTestLoop() {
  if (!petHitTestTimer) return;
  clearInterval(petHitTestTimer);
  petHitTestTimer = null;
}

function startPetRecoveryLoop() {
  if (petRecoveryTimer) return;
  petRecoveryTimer = setInterval(() => {
    if (quitting) return;
    if (panelWindow && !panelWindow.isDestroyed() && panelWindow.isVisible()) return;
    if (!petWindow || petWindow.isDestroyed()) {
      if (petWindowCreating) return;
      try {
        createPetWindow();
      } catch (e) {
        petWindow = null;
        petWindowCreating = false;
      }
      return;
    }
    try {
      if (!petWindow.isVisible()) petWindow.showInactive();
    } catch (e) {}
  }, PET_RECOVERY_INTERVAL);
}

function stopPetRecoveryLoop() {
  if (!petRecoveryTimer) return;
  clearInterval(petRecoveryTimer);
  petRecoveryTimer = null;
}

ipcMain.on("toggle-panel", () => {
  try {
    togglePanel();
  } catch (e) {
    // 避免主进程因窗口生命周期异常被打崩
  }
});

// 桌宠窗口 -> 主面板窗口：转发 Agent 结构化事件（复用现有 SSE，不额外开新通道）
ipcMain.on("agent-event", (_event, payload) => {
  if (!payload) return;
  if (!panelWindow || panelWindow.isDestroyed()) return;
  try {
    panelWindow.webContents.send("agent-event", payload);
  } catch (e) {}
});

ipcMain.on("panel-window-control", (event, payload) => {
  if (!panelWindow || panelWindow.isDestroyed()) return;
  const action = payload?.action;
  if (action === "minimize") {
    try { panelWindow.minimize(); } catch (e) {}
    return;
  }
  if (action === "toggle-maximize") {
    try {
      if (panelWindow.isMaximized()) {
        panelWindow.unmaximize();
      } else {
        panelWindow.maximize();
      }
    } catch (e) {}
    return;
  }
  if (action === "close") {
    // 不真正退出：隐藏主面板并恢复桌宠窗口
    try {
      if (panelWindow.isVisible()) panelWindow.hide();
    } catch (e) {}
    restorePetWindow();
  }
});


function createPanelWindow() {
  panelWindow = new BrowserWindow({
    width: 960,
    height: 720,
    show: false,
    frame: false,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });

  // 主面板直接加载 panel.html（世界/状态两页都在这里），避免入口页多一步跳转
  panelWindow.loadFile(path.join(__dirname, "..", "renderer", "panel.html"));
  // 关闭主面板窗口时不要直接退出：隐藏面板并恢复桌宠窗口，符合“桌面伴生”使用习惯。
  panelWindow.on("close", (event) => {
    if (quitting) return;
    try {
      event.preventDefault();
    } catch (e) {}
    try {
      panelWindow.hide();
    } catch (e) {}
    restorePetWindow();
  });
  panelWindow.on("closed", () => {
    panelWindow = null;
  });
}

function closePetWindow() {
  if (!petWindow || petWindow.isDestroyed()) return;
  try {
    petWindow.close();
  } catch (e) {
    try {
      petWindow.destroy();
    } catch (err) {}
    petWindow = null;
  }
}

function hidePetWindow() {
  if (!petWindow || petWindow.isDestroyed()) return;
  try {
    petWindow.hide();
  } catch (e) {
    // hide 失败时再尝试强制关闭，避免透明窗口残留遮挡
    closePetWindow();
  }
}

function restorePetWindow() {
  if (quitting) return;
  if (petWindowCreating) return;
  if (petWindow && !petWindow.isDestroyed()) {
    try {
      petWindow.showInactive();
    } catch (e) {}
    return;
  }
  try {
    createPetWindow();
  } catch (e) {
    petWindow = null;
    petWindowCreating = false;
  }
}

function togglePanel() {
  // panelWindow 可能已被销毁但变量未清空（Windows 上会直接抛 "Object has been destroyed"）
  if (!panelWindow || panelWindow.isDestroyed()) {
    try {
      createPanelWindow();
    } catch (e) {
      panelWindow = null;
      return;
    }
  }
  if (panelWindow.isVisible()) {
    panelWindow.hide();
    // 退出主面板时恢复桌宠悬浮窗口，便于继续交互
    restorePetWindow();
  } else {
    panelWindow.show();
    panelWindow.focus();
    // 打开主面板时关闭桌宠“悬浮窗口形态”，改为仅在世界页右下角展示
    hidePetWindow();
  }
}

function createTray() {
  const trayIcon = nativeImage.createEmpty();
  tray = new Tray(trayIcon);
  tray.setToolTip("智能体");

  const contextMenu = Menu.buildFromTemplate([
    { label: "打开主面板", click: togglePanel },
    { type: "separator" },
    { label: "退出", role: "quit" }
  ]);

  tray.setContextMenu(contextMenu);
  tray.on("click", togglePanel);
}

app.whenReady().then(() => {
  startBackend();
  createPetWindow();
  createPanelWindow();
  createTray();
  startPetRecoveryLoop();
});

app.on("window-all-closed", (event) => {
  event.preventDefault();
});

app.on("before-quit", (event) => {
  // Electron 退出时，优先让后端把 running 状态落库为 stopped，避免下次启动 UI 卡在“执行中”。
  if (quitting) return;
  quitting = true;
  try { event.preventDefault(); } catch (e) {}
  stopPetRecoveryLoop();
  stopRunningTasksOnExit(800).finally(() => {
    stopBackend();
    // 使用 exit 避免 before-quit 递归触发
    app.exit(0);
  });
});
