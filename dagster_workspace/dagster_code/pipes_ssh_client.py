import subprocess
import shlex
import json
from contextlib import contextmanager

from dagster import AssetExecutionContext, ConfigurableResource
from dagster._core.pipes.client import PipesClient, PipesClientCompletedInvocation
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesMessageReader,
    open_pipes_session,
)
from dagster_pipes import encode_param

SSH_HOST     = "10.10.159.74"
SSH_USER     = "bcp_runner"
SSH_KEY_PATH = "/home/dagster_user/.ssh/dagster_to_vm1"


class PipesSSHStdioMessageReader(PipesMessageReader):
    def __init__(self):
        self._collected_lines: list[str] = []
 
    def add_line(self, line: str) -> None:
        self._collected_lines.append(line)
 
    @contextmanager
    def read_messages(self, handler):
        self._handler = handler
        try:
            yield {"path": None}
        finally:
            for line in self._collected_lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(message, dict) and "__dagster_pipes_version" in message:
                    self._handler.handle_message(message)
 
    def no_messages_debug_text(self) -> str:
        return "PipesSSHStdioMessageReader 未收到任何 Pipes 訊息。"
 
 
class PipesSSHClient(PipesClient, ConfigurableResource):
    ssh_host: str
    ssh_user: str
    ssh_key_path: str
    timeout_seconds: int = 7200
 
    def run(
        self,
        *,
        context: AssetExecutionContext,
        command: list[str],
        extras: dict = None,
    ) -> PipesClientCompletedInvocation:
        message_reader = PipesSSHStdioMessageReader()
 
        with open_pipes_session(
            context=context,
            message_reader=message_reader,
            context_injector=PipesEnvContextInjector(),
            extras=extras or {},
        ) as session:
            env_vars = session.get_bootstrap_env_vars()
 
            env_vars["DAGSTER_PIPES_MESSAGES"] = encode_param({"stdio": "stdout"})
 
            # ------------------------------------------------------------
            # 安全性關鍵：command / env_vars 裡的每一個值都可能來自
            # config（路徑、delimiter、table 名稱...），這些值最終會被
            # SSH 送到遠端交給 shell 執行。如果不逐一跳脫，任何一個值
            # 含有 ; ` $() | & 等 shell 特殊字元，都會被遠端 shell
            # 解讀成額外指令 —— 這是指令注入(command injection)。
            # shlex.quote() 會把每個值包成該 shell 安全的單一 token，
            # 讓它「不管內容是什麼」都只會被當成一個字面值參數。
            # ------------------------------------------------------------
            env_assignments = " ".join(
                f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items()
            )
            quoted_command = shlex.join(str(c) for c in command)
            remote_command = f"{env_assignments} {quoted_command}"
 
            ssh_command = [
                "ssh",
                "-i", self.ssh_key_path,
                "-o", "StrictHostKeyChecking=no",
                "-o", "ServerAliveInterval=60",
                "-o", "ServerAliveCountMax=120",
                f"{self.ssh_user}@{self.ssh_host}",
                remote_command,
            ]
 
            context.log.info(f"[SSH → {self.ssh_host}] 執行: {' '.join(command)}")
 
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
 
            for line in result.stdout.splitlines():
                message_reader.add_line(line)
                context.log.info(f"[SSH stdout] {line}")
 
            if result.stderr:
                context.log.warning(f"[SSH stderr]\n{result.stderr}")
 
            if result.returncode != 0:
                raise Exception(
                    f"SSH 遠端執行失敗 (exit {result.returncode}): {result.stderr}"
                )
 
        return PipesClientCompletedInvocation(session)
 