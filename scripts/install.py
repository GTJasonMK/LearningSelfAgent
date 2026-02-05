#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
一键安装前后端依赖
使用 uv 包管理器和虚拟环境
"""

import subprocess
import sys
import os
from pathlib import Path


def get_project_root():
    """获取项目根目录"""
    return Path(__file__).parent.parent.absolute()


def run_command(cmd, cwd=None, check=True):
    """执行命令并打印输出"""
    print(f"\n>>> 执行: {' '.join(cmd)}")
    print(f"    目录: {cwd or os.getcwd()}")
    print("-" * 50)
    result = subprocess.run(cmd, cwd=cwd, check=check)
    return result.returncode == 0


def run_command_with_env(cmd, cwd=None, env=None, check=True):
    """执行命令并打印输出（支持自定义环境变量）"""
    print(f"\n>>> 执行: {' '.join(cmd)}")
    print(f"    目录: {cwd or os.getcwd()}")
    print("-" * 50)
    result = subprocess.run(cmd, cwd=cwd, env=env, check=check)
    return result.returncode == 0


def check_uv_installed():
    """检查 uv 是否已安装"""
    try:
        subprocess.run(["uv", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def check_node_installed():
    """检查 Node.js 是否已安装"""
    try:
        if sys.platform == "win32":
            subprocess.run(["node.exe", "--version"], capture_output=True, check=True, shell=True)
        else:
            subprocess.run(["node", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def install_uv():
    """安装 uv 包管理器"""
    print("\n[1/4] 检查 uv 包管理器...")

    if check_uv_installed():
        print("uv 已安装")
        return True

    print("正在安装 uv...")
    if sys.platform == "win32":
        # Windows 使用 pip 安装
        return run_command([sys.executable, "-m", "pip", "install", "uv"], check=False)
    else:
        # Unix 系统使用官方脚本（避免把 `|` 当作参数传给 curl）
        try:
            curl = subprocess.Popen(
                ["curl", "-LsSf", "https://astral.sh/uv/install.sh"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            subprocess.run(["sh"], stdin=curl.stdout, check=False)
            try:
                curl.stdout.close()
            except Exception:
                pass
            try:
                curl.wait(timeout=30)
            except Exception:
                pass
            return check_uv_installed()
        except Exception:
            return False


def setup_backend_venv(project_root):
    """创建后端虚拟环境并安装依赖"""
    print("\n[2/4] 设置后端虚拟环境...")

    # 约定：Windows 使用 `.venv-win`，WSL/Linux 使用 `.venv`，避免共用 `.venv/pyvenv.cfg` 互相污染
    is_windows_like = (
        os.name == "nt"
        or sys.platform.startswith("win")
        or sys.platform.startswith("cygwin")
        or sys.platform.startswith("msys")
    )
    venv_dir_override = str(os.environ.get("LSA_VENV_DIR") or "").strip().strip("\"").strip("'").strip()
    if venv_dir_override:
        venv_path = Path(venv_dir_override) if Path(venv_dir_override).is_absolute() else (project_root / venv_dir_override)
    else:
        venv_path = project_root / (".venv-win" if is_windows_like else ".venv")
    requirements_path = project_root / "backend" / "requirements.txt"

    # 创建虚拟环境
    if not venv_path.exists():
        print("创建虚拟环境...")
        if not run_command(["uv", "venv", str(venv_path)], cwd=str(project_root), check=False):
            # 回退到 python -m venv
            print("uv venv 失败，尝试使用 python -m venv...")
            run_command([sys.executable, "-m", "venv", str(venv_path)], cwd=str(project_root))
    else:
        print(f"虚拟环境已存在: {venv_path}")

    venv_python = (
        venv_path / "Scripts" / "python.exe"
        if is_windows_like
        else venv_path / "bin" / "python"
    )

    # 安装后端依赖
    print("安装后端依赖...")
    if check_uv_installed():
        run_command(
            ["uv", "pip", "install", "-p", str(venv_python), "-r", str(requirements_path)],
            cwd=str(project_root),
            check=False
        )
    else:
        # 回退到 pip
        if is_windows_like:
            pip_path = venv_path / "Scripts" / "pip.exe"
        else:
            pip_path = venv_path / "bin" / "pip"
        run_command([str(pip_path), "install", "-r", str(requirements_path)], check=False)

    return True


def setup_frontend(project_root):
    """安装前端依赖"""
    print("\n[3/4] 检查 Node.js...")

    if not check_node_installed():
        print("错误: Node.js 未安装，请先安装 Node.js")
        print("下载地址: https://nodejs.org/")
        return False

    print("Node.js 已安装")

    print("\n[4/4] 安装前端依赖...")
    frontend_path = project_root / "frontend-gui"

    # 检查 package.json 是否存在
    if not (frontend_path / "package.json").exists():
        print(f"错误: 未找到 {frontend_path / 'package.json'}")
        return False

    # 设置 electron 镜像（国内加速）
    env = os.environ.copy()
    env["ELECTRON_MIRROR"] = "https://npmmirror.com/mirrors/electron/"
    env["ELECTRON_BUILDER_BINARIES_MIRROR"] = "https://npmmirror.com/mirrors/electron-builder-binaries/"
    print("使用 npmmirror 镜像加速 electron 下载...")

    # 使用 npm install（Windows 需要 npm.cmd）
    if sys.platform == "win32":
        success = run_command_with_env(["npm.cmd", "install"], cwd=str(frontend_path), env=env, check=False)
    else:
        success = run_command_with_env(["npm", "install"], cwd=str(frontend_path), env=env, check=False)

    # 检查 electron 是否安装成功
    electron_path = frontend_path / "node_modules" / ".bin" / ("electron.cmd" if sys.platform == "win32" else "electron")
    if not electron_path.exists():
        print("\n[警告] electron 未正确安装，尝试单独安装...")
        if sys.platform == "win32":
            run_command_with_env(["npm.cmd", "install", "electron", "--save-dev"], cwd=str(frontend_path), env=env, check=False)
        else:
            run_command_with_env(["npm", "install", "electron", "--save-dev"], cwd=str(frontend_path), env=env, check=False)

        # 再次检查
        if not electron_path.exists():
            print("[错误] electron 安装失败")
            print("请手动执行以下命令：")
            print("  set ELECTRON_MIRROR=https://npmmirror.com/mirrors/electron/")
            print("  cd frontend-gui && npm install electron --save-dev")
            return False

    print("[成功] 前端依赖安装完成，electron 已就绪")
    return True


def main():
    """主函数"""
    # Windows 下设置控制台编码
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        ctypes.windll.kernel32.SetConsoleCP(65001)

    print("=" * 60)
    print("LearningSelfAgent 依赖安装脚本")
    print("=" * 60)

    project_root = get_project_root()
    print(f"项目根目录: {project_root}")

    # 1. 安装 uv
    install_uv()

    # 2. 设置后端
    setup_backend_venv(project_root)

    # 3. 设置前端
    setup_frontend(project_root)

    print("\n" + "=" * 60)
    print("安装完成!")
    print("=" * 60)
    print("\n后续步骤:")
    print("  1. 运行项目: python scripts/start.py")
    print("  2. 或手动启动:")
    print("     - 后端: uvicorn backend.src.main:app --reload --port 8123")
    print("     - 前端: cd frontend-gui && npm run start")
    print("")


if __name__ == "__main__":
    main()
