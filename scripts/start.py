#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
一键启动项目
同时启动后端 (FastAPI) 和前端 (Electron)
"""

import subprocess
import sys
import os
import time
import signal
import threading
from pathlib import Path


# 全局进程列表，用于清理
processes = []


def get_project_root():
    """获取项目根目录"""
    return Path(__file__).parent.parent.absolute()


def get_venv_python(project_root):
    """获取虚拟环境中的 Python 路径"""
    # 允许用户强制指定 Python（避免 MSYS/WSL/Windows 混用导致路径被错误识别）
    override_raw = str(os.environ.get("LSA_PYTHON") or "").strip()
    if override_raw:
        # 用户常会带引号：做一次去引号，避免 subprocess 找不到可执行文件
        override = override_raw.strip().strip("\"").strip("'").strip()

        # 如果 override 看起来像“路径”，但实际不存在，就忽略它并继续走 venv 探测。
        # 典型坏例子：Git Bash/MSYS 把 python 暴露成 /usr/bin/python.exe，但在普通 Windows 进程里不可用；
        # 以及用户误把值写成带引号的字符串，导致出现 '"...python.exe' 这种路径。
        looks_like_path = (
            ("/" in override)
            or ("\\" in override)
            or (":" in override)
            or override.lower().endswith(".exe")
        )
        if not looks_like_path:
            return override

        try:
            if Path(override).exists():
                return override
            # 兼容 “/usr/bin\python.exe” 这种混合分隔符（先纠正一次再判断）
            if override.startswith("/") and ("\\" in override):
                fixed = override.replace("\\", "/")
                if Path(fixed).exists():
                    return fixed
        except Exception:
            # exists 检测失败时宁可返回 override，让用户自己发现问题并修正
            return override

    # venv 探测：需要区分“Windows 类环境”和“类 Unix 环境”，并避免 WSL/Windows 共用同一个 `.venv` 目录导致互相污染。
    #
    # 背景：仓库位于 Windows 盘符（例如 E:）时，WSL 与 Windows 会共享同一份 `.venv/pyvenv.cfg`。
    # 一旦在 WSL 里执行 `uv venv .venv`，cfg 里的 `home=/usr/bin` 会让 Windows 侧的 `.venv\\Scripts\\python.exe`
    # 解析出 `/usr/bin\\python.exe` 并直接报 “No Python at ...”。反过来也是同理。
    #
    # 解决：约定 Windows 使用 `.venv-win`，WSL/Linux 使用 `.venv`；并按 `pyvenv.cfg home` 做一次平台匹配校验。
    is_windows_like = (
        os.name == "nt"
        or sys.platform.startswith("win")
        or sys.platform.startswith("cygwin")
        or sys.platform.startswith("msys")
    )

    def _read_venv_home(venv_dir: Path):
        cfg = venv_dir / "pyvenv.cfg"
        if not cfg.exists():
            return None
        try:
            for line in cfg.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.strip().startswith("home"):
                    parts = line.split("=", 1)
                    if len(parts) == 2:
                        return parts[1].strip()
        except Exception:
            return None
        return None

    def _venv_matches_platform(venv_dir: Path):
        home = _read_venv_home(venv_dir)
        if not home:
            return True
        if is_windows_like:
            return not home.startswith("/")
        return (":" not in home) and ("\\" not in home)

    # 允许覆盖 venv 目录（例如 CI/自定义环境），相对路径默认以项目根目录为基准
    venv_dir_override = str(os.environ.get("LSA_VENV_DIR") or "").strip().strip("\"").strip("'").strip()
    venv_dirs = []
    if venv_dir_override:
        venv_dirs.append(Path(venv_dir_override) if Path(venv_dir_override).is_absolute() else (project_root / venv_dir_override))
    else:
        if is_windows_like:
            venv_dirs = [project_root / ".venv-win", project_root / ".venv"]
        else:
            venv_dirs = [project_root / ".venv", project_root / ".venv-win"]

    for venv_dir in venv_dirs:
        try:
            if not venv_dir.exists():
                continue
            if not _venv_matches_platform(venv_dir):
                continue
        except Exception:
            continue

        candidates = []
        if is_windows_like:
            candidates = [
                venv_dir / "Scripts" / "python.exe",
                venv_dir / "Scripts" / "python",
            ]
        else:
            candidates = [
                venv_dir / "bin" / "python",
            ]

        for candidate in candidates:
            try:
                if candidate.exists():
                    return str(candidate)
            except Exception:
                continue

    return sys.executable


def stream_output(process, prefix, color_code):
    """实时输出进程的标准输出"""
    try:
        for line in iter(process.stdout.readline, b''):
            if line:
                # 尝试多种编码
                for encoding in ['utf-8', 'gbk', 'cp936', 'latin-1']:
                    try:
                        text = line.decode(encoding).rstrip()
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    text = line.decode('utf-8', errors='replace').rstrip()
                print(f"\033[{color_code}m[{prefix}]\033[0m {text}")
    except Exception:
        pass


def stream_error(process, prefix, color_code):
    """实时输出进程的错误输出"""
    try:
        for line in iter(process.stderr.readline, b''):
            if line:
                # 尝试多种编码
                for encoding in ['utf-8', 'gbk', 'cp936', 'latin-1']:
                    try:
                        text = line.decode(encoding).rstrip()
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    text = line.decode('utf-8', errors='replace').rstrip()
                print(f"\033[{color_code}m[{prefix}]\033[0m {text}")
    except Exception:
        pass


def start_backend(project_root):
    """启动后端服务"""
    print("\n[后端] 启动 FastAPI 服务...")

    venv_python = get_venv_python(project_root)
    print(f"[后端] Python: {venv_python}")

    # 使用 uvicorn 启动
    cmd = [
        venv_python, "-m", "uvicorn",
        "backend.src.main:app",
        "--port", "8123",
        "--host", "127.0.0.1"
    ]

    enable_reload = str(os.environ.get("LSA_BACKEND_RELOAD") or "").strip() == "1"
    if enable_reload:
        cmd.extend([
            "--reload",
            "--reload-dir", "backend/src",
            "--reload-dir", "backend/prompt",
            "--reload-dir", "shared",
        ])

    env = os.environ.copy()
    if enable_reload:
        # 兼容旧版 uvicorn：通过 watchfiles 环境变量忽略实验目录，避免临时脚本触发重载
        ignore_patterns = env.get("WATCHFILES_IGNORE", "")
        extra_ignore = "backend/.agent/workspace/*,backend/.agent/workspace/**,backend\\.agent\\workspace\\*,backend\\.agent\\workspace\\**"
        env["WATCHFILES_IGNORE"] = f"{ignore_patterns},{extra_ignore}".strip(",")

    process = subprocess.Popen(
        cmd,
        cwd=str(project_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1
    )

    processes.append(process)

    # 启动输出线程
    threading.Thread(target=stream_output, args=(process, "后端", "36"), daemon=True).start()
    threading.Thread(target=stream_error, args=(process, "后端", "36"), daemon=True).start()

    return process


def start_frontend(project_root):
    """启动前端应用"""
    print("\n[前端] 启动 Electron 应用...")

    frontend_path = project_root / "frontend-gui"

    # 检查 node_modules
    if not (frontend_path / "node_modules").exists():
        print("[前端] 错误: node_modules 不存在，请先运行 install.py")
        return None

    # 检查 electron 是否已安装
    electron_path = frontend_path / "node_modules" / ".bin" / ("electron.cmd" if sys.platform == "win32" else "electron")
    if not electron_path.exists():
        print("[前端] 错误: electron 未安装，请先运行 install.py")
        return None

    # 使用 npx electron . 来启动，更可靠
    if sys.platform == "win32":
        cmd = ["npx.cmd", "electron", "."]
    else:
        cmd = ["npx", "electron", "."]

    # Windows 下设置环境变量以避免编码问题
    env = os.environ.copy()
    if sys.platform == "win32":
        env["PYTHONIOENCODING"] = "utf-8"
    # 后端已由本脚本启动，前端（Electron）无需重复启动后端，避免端口占用错误
    env["LSA_SKIP_BACKEND"] = "1"

    process = subprocess.Popen(
        cmd,
        cwd=str(frontend_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        env=env
    )

    processes.append(process)

    # 启动输出线程
    threading.Thread(target=stream_output, args=(process, "前端", "35"), daemon=True).start()
    threading.Thread(target=stream_error, args=(process, "前端", "35"), daemon=True).start()

    return process


def cleanup(signum=None, frame=None):
    """清理所有子进程"""
    print("\n\n正在关闭所有服务...")
    for proc in processes:
        try:
            if sys.platform == "win32":
                proc.terminate()
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            pass

    # 等待进程结束
    for proc in processes:
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()

    print("所有服务已关闭")
    sys.exit(0)


def wait_for_backend(timeout=30):
    """等待后端服务就绪"""
    import urllib.request
    import urllib.error

    print("\n等待后端服务就绪...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = urllib.request.urlopen("http://127.0.0.1:8123/api/health", timeout=2)
            if response.status == 200:
                print("[后端] 服务已就绪!")
                return True
        except (urllib.error.URLError, Exception):
            pass
        time.sleep(1)
        print(".", end="", flush=True)

    print("\n[警告] 后端服务启动超时，继续启动前端...")
    return False


def main():
    """主函数"""
    # Windows 下设置控制台编码
    if sys.platform == "win32":
        import ctypes
        # 设置控制台代码页为 UTF-8
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        ctypes.windll.kernel32.SetConsoleCP(65001)

    print("=" * 60)
    print("LearningSelfAgent 启动脚本")
    print("=" * 60)
    print("按 Ctrl+C 停止所有服务")

    project_root = get_project_root()
    print(f"项目根目录: {project_root}")

    # 注册信号处理
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # 启动后端
    backend_proc = start_backend(project_root)
    if not backend_proc:
        print("后端启动失败")
        return

    # 等待后端就绪
    wait_for_backend()

    # 启动前端
    frontend_proc = start_frontend(project_root)
    if not frontend_proc:
        print("前端启动失败")
        cleanup()
        return

    print("\n" + "=" * 60)
    print("服务已启动:")
    print("  - 后端 API: http://127.0.0.1:8123")
    print("  - API 文档: http://127.0.0.1:8123/docs")
    print("  - 前端应用: Electron 窗口")
    print("=" * 60)
    print("\n按 Ctrl+C 停止所有服务\n")

    # 等待进程
    try:
        while True:
            # 检查进程状态
            backend_alive = backend_proc.poll() is None
            frontend_alive = frontend_proc and frontend_proc.poll() is None

            if not backend_alive and not frontend_alive:
                print("\n所有服务已停止")
                break

            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()


if __name__ == "__main__":
    main()
