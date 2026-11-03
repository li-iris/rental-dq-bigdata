#!/usr/bin/env bash
# macOS 本地环境：Java 17 + Python 虚拟环境 + PySpark 3.5
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if ! /usr/libexec/java_home -v 17 >/dev/null 2>&1; then
  echo "安装 Java 17（需要 Homebrew）..."
  brew install openjdk@17
  sudo ln -sfn "$(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk" /Library/Java/JavaVirtualMachines/openjdk-17.jdk
fi
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
python3 -m venv "$ROOT/.venv"
source "$ROOT/.venv/bin/activate"
pip install -r "$ROOT/requirements.txt"
echo
echo "环境就绪。之后每次打开新终端先执行："
echo "  source .venv/bin/activate && export JAVA_HOME=\$(/usr/libexec/java_home -v 17)"
