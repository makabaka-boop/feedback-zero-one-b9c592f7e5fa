"""湖表压实后端验收测试（仅用标准库 + 真实 HTTP 子进程）。

覆盖：
  1. 非法请求（坏 JSON / 重复成员 / 未知成员 / 类型 / 引用 / 标识复用）
  2. 追加、读取（null、重复值、int64 边界、排序、历史版本）
  3. 删除（越界、重复/已删坐标、空数组仍增版本、已替换文件拒绝）
  4. 规格示例 a=[7,7] -> 准备 j -> 删 [a,0] -> 发布
  5. 交错删除：准备之后、发布之前的删除在发布时映射到新位置
  6. 竞争压实：重叠输入的两个作业，先发布者成功，后发布者 409 且状态不变
  7. 重启验收：正常 SIGTERM 重启后全状态保持；准备写盘后 / 发布提交前后强退恢复
  8. 并发交错：多线程混合请求下的一致性
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LAKECOMP_CRASH = ""

PASS = 0
FAIL = 0
FAILURES: list[str] = []


def check(cond: bool, label: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        FAILURES.append(label)
        print(f"  ✗ {label}")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, data_dir: str, crash: str = ""):
        self.data_dir = data_dir
        self.port = free_port()
        # 干净基线环境 + 白名单，杜绝故障注入等变量泄漏到重启进程
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": os.environ.get("HOME", "/tmp"),
            "LANG": os.environ.get("LANG", "C.UTF-8"),
            "PYTHONPATH": str(REPO_ROOT),
        }
        if crash:
            env["LAKECOMP_CRASH"] = crash
        self.proc = subprocess.Popen(
            [sys.executable, "-P", "-m", "lakecomp.server",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--data", self.data_dir],
            cwd=REPO_ROOT, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self._wait_ready()

    def _wait_ready(self, timeout: float = 15.0) -> None:
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out, err = self.proc.communicate(timeout=5)
                raise RuntimeError(
                    f"服务提前退出 code={self.proc.returncode}\n"
                    f"STDOUT={out.decode(errors='replace')}\n"
                    f"STDERR={err.decode(errors='replace')}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.5):
                    return
            except OSError as e:
                last_err = e
                time.sleep(0.1)
        raise RuntimeError(f"服务未在 {timeout}s 内就绪: {last_err}")

    def post(self, path: str, payload, raw: bytes | None = None):
        if raw is None:
            data = json.dumps(payload).encode("utf-8")
        else:
            data = raw
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            rc = self.proc.poll()
            detail = ""
            if rc is not None:
                try:
                    _, err = self.proc.communicate(timeout=5)
                    detail = "\nSERVER STDERR:\n" + err.decode(errors="replace")[-3000:]
                except Exception:
                    pass
            raise AssertionError(
                f"请求 {path} 失败: {type(e).__name__}: {e}; 服务退出码={rc}{detail}") from e

    def stop(self, sig: int = 15) -> None:
        self.proc.send_signal(sig)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)

    def hard_kill(self) -> None:
        self.proc.kill()
        self.proc.wait(timeout=5)

    def post_expect_crash(self, path: str, payload) -> None:
        """对预期触发强退（os._exit 137）的请求，容忍连接断开。"""
        try:
            self.post(path, payload)
        except AssertionError as e:
            if "137" not in str(e):
                raise

    def stderr_tail(self) -> str:
        try:
            return self.proc.stderr.read().decode(errors="replace")[-2000:]  # type: ignore[union-attr]
        except Exception:
            return ""


def expect_error(srv: Server, path: str, payload, status: int, label: str,
                 raw: bytes | None = None) -> None:
    code, body = srv.post(path, payload, raw=raw)
    check(code == status, f"{label} -> {status}（实际 {code} {body}）")
    check(isinstance(body, dict) and "path" in body and "error" in body,
          f"{label} 错误体含 path/error（实际 {body}）")


def rows_of(body: dict) -> list[dict]:
    return body["rows"]


def section(title: str) -> None:
    print(f"\n=== {title} ===")


# ---------------------------------------------------------------- 1. 非法请求
def test_validation(srv: Server) -> None:
    section("1. 非法请求处理（400/409，拒绝不改状态）")
    v0 = srv.post("/read", {"version": 0})[1]
    check(v0 == {"version": 0, "rows": []}, "初态版本 0、空表")

    expect_error(srv, "/append", None, 400, "坏 JSON 语法", raw=b"{not json")
    bad_dup, _ = srv.post("/append", None, raw=b'{"file":"x","file":"y","values":[]}')
    check(bad_dup == 400, "重复成员 -> 400")
    expect_error(srv, "/append", {"file": "a", "values": [], "extra": 1}, 400, "未知成员")
    expect_error(srv, "/append", {"file": "", "values": []}, 400, "空 file")
    expect_error(srv, "/append", {"file": "a-b", "values": []}, 400, "file 含非字母数字")
    expect_error(srv, "/append", {"file": 1, "values": []}, 400, "file 类型错误")
    expect_error(srv, "/append", {"file": "a", "values": "x"}, 400, "values 非数组")
    expect_error(srv, "/append", {"file": "a", "values": [True]}, 400,
                 "布尔禁止（整数禁布尔）")
    expect_error(srv, "/append", {"file": "a", "values": [1.5]}, 400, "浮点拒绝")
    expect_error(srv, "/append", {"file": "a", "values": ["7"]}, 400, "字符串拒绝")
    expect_error(srv, "/append", {"file": "a", "values": [2**63]}, 400, "int64 上越界")
    expect_error(srv, "/append", {"file": "a", "values": [-(2**63) - 1]}, 400,
                 "int64 下越界")
    expect_error(srv, "/delete", {"positions": [["a", 0]]}, 400, "删除不存在文件")
    expect_error(srv, "/prepare", {"job": "j", "files": []}, 400, "空 files")
    expect_error(srv, "/prepare", {"job": "j", "files": ["nope"]}, 400,
                 "prepare 引用未知文件")
    expect_error(srv, "/publish", {"job": "ghost"}, 400, "publish 未知作业")
    expect_error(srv, "/read", {"version": -1}, 400, "负版本")
    expect_error(srv, "/read", {"version": 1}, 400, "未来版本")
    expect_error(srv, "/read", {"version": "0"}, 400, "version 类型错误")
    code404, _ = srv.post("/nope", {})
    check(code404 == 404, "未知路径 404")

    check(srv.post("/read", {"version": 0})[1] == {"version": 0, "rows": []},
          "全部拒绝后状态不变（仍为版本 0 空表）")


# ---------------------------------------------------------------- 2. 追加/读取
def test_append_read(srv: Server) -> None:
    section("2. 追加与读取")
    code, b = srv.post("/append", {"file": "alpha",
                                   "values": [None, 7, -(2**63), 2**63 - 1, 7]})
    check(code == 200 and b["version"] == 1, f"追加 alpha -> version 1（实际 {b}）")
    code, b = srv.post("/append", {"file": "beta", "values": []})
    check(code == 200 and b["version"] == 2, "空 values 追加也增版本")
    code, b = srv.post("/append", {"file": "alpha", "values": [1]})
    check(code == 409, f"file 标识复用 -> 409（实际 {code} {b}）")

    body = srv.post("/read", {"version": 2})[1]
    expect = [
        {"file": "alpha", "pos": 0, "value": None},
        {"file": "alpha", "pos": 1, "value": 7},
        {"file": "alpha", "pos": 2, "value": -(2**63)},
        {"file": "alpha", "pos": 3, "value": 2**63 - 1},
        {"file": "alpha", "pos": 4, "value": 7},
    ]
    check(rows_of(body) == expect, "读取含 null/重复值/int64 边界且按 file、pos 排序")
    check(srv.post("/read", {"version": 1})[1] == {"version": 1, "rows": expect},
          "历史版本 1 永久可读")
    check(srv.post("/read", {"version": 0})[1] == {"version": 0, "rows": []},
          "历史版本 0 永久可读")


# ---------------------------------------------------------------- 3. 删除
def test_delete(srv: Server) -> None:
    section("3. 删除语义")
    code, b = srv.post("/delete", {"positions": [["alpha", 1], ["alpha", 1],
                                                  ["alpha", 0]]})
    check(code == 200 and b["version"] == 3, f"重复/已删坐标允许 -> version 3（实际 {b}）")
    code, b = srv.post("/delete", {"positions": []})
    check(code == 200 and b["version"] == 4, "空 positions 也成功并增版本")
    expect_error(srv, "/delete", {"positions": [["alpha", 5]]}, 400, "pos 越界")
    expect_error(srv, "/delete", {"positions": [["alpha", -1]]}, 400, "负 pos")
    expect_error(srv, "/delete", {"positions": [["beta", 0]]}, 400, "空文件界内无行")
    expect_error(srv, "/delete", {"positions": [["alpha", True]]}, 400, "布尔 pos 拒绝")

    body = srv.post("/read", {"version": 4})[1]
    check(rows_of(body) == [
        {"file": "alpha", "pos": 2, "value": -(2**63)},
        {"file": "alpha", "pos": 3, "value": 2**63 - 1},
        {"file": "alpha", "pos": 4, "value": 7},
    ], "删除后剩余行正确（pos 不压缩）")
    # 越界请求是事务前校验，版本不应增加
    check(srv.post("/read", {"version": 4})[0] == 200, "拒绝请求未推进版本")


# ---------------------------------------------------------------- 4. 规格示例
def test_spec_example(srv: Server) -> None:
    section("4. 规格示例：a=[7,7]，准备 j 后删 [a,0] 再发布")
    _, b = srv.post("/append", {"file": "a", "values": [7, 7]})
    check(b["version"] == 5, f"追加 a -> version 5（实际 {b}）")
    _, b = srv.post("/prepare", {"job": "j", "files": ["a"]})
    check(b == {"base": 5}, f"准备固定 base=5 且不增版本（实际 {b}）")
    _, b = srv.post("/delete", {"positions": [["a", 0]]})
    check(b["version"] == 6, "准备后删除 -> version 6")
    _, b = srv.post("/publish", {"job": "j"})
    check(b["version"] == 7, f"发布 -> version 7（实际 {b}）")
    body = srv.post("/read", {"version": 7})[1]
    jrows = [r for r in rows_of(body) if r["file"] == "~j"]
    check(jrows == [{"file": "~j", "pos": 1, "value": 7}],
          f"仅余 {{file:~j,pos:1,value:7}}（实际 ~j 行 {jrows}）")
    check(not any(r["file"] == "a" for r in rows_of(body)),
          "输入文件 a 已被替换，读取中不再出现")
    # 同 job 发布重试幂等
    _, b2 = srv.post("/publish", {"job": "j"})
    check(b2 == {"version": 7}, f"同 job 发布重试返回原版本 7（实际 {b2}）")
    check(srv.post("/read", {"version": 7})[1] == body, "重试不改变状态")
    # 已替换文件不能再被引用
    expect_error(srv, "/delete", {"positions": [["a", 1]]}, 400,
                 "发布后旧文件坐标不可再删")
    expect_error(srv, "/prepare", {"job": "j", "files": ["a"]}, 409,
                 "job 标识永不复用")
    # 历史版本仍可完整读取
    hist5 = srv.post("/read", {"version": 5})[1]
    check(len(hist5["rows"]) == 3 + 2, "版本 5 历史含 alpha 剩余 3 行与 a 的 2 行")


# ---------------------------------------------------------------- 5. 交错删除
def test_interleaved_delete(srv: Server) -> None:
    section("5. 交错删除：准备与发布之间对输入文件的删除映射到新位置")
    srv.post("/append", {"file": "a2", "values": [10, 20, 30]})
    srv.post("/append", {"file": "b2", "values": [40]})
    srv.post("/append", {"file": "c2", "values": [50, 60, 70, 80]})

    _, prep1 = srv.post("/prepare", {"job": "j1", "files": ["a2", "c2"]})
    _, prep2 = srv.post("/prepare", {"job": "j2", "files": ["c2"]})
    check("base" in prep1 and "base" in prep2 and prep1["base"] == prep2["base"],
          f"两个作业可交错准备，固定同一 base（实际 {prep1} {prep2}）")

    # 准备之后删除：a2 的一行（映射内）、c2 的 pos=2（两个作业映射内都有）
    _, d1 = srv.post("/delete", {"positions": [["a2", 0], ["c2", 2]]})
    check(d1["version"] > prep1["base"], "交错删除推进版本但不影响已准备的作业")

    _, pub1 = srv.post("/publish", {"job": "j1"})
    v_pub1 = pub1["version"]
    body = srv.post("/read", {"version": v_pub1})[1]
    # ~j1 合并坐标 0..6 = a2(10,20,30) + c2(50,60,70,80)，映射覆盖全部 7 行；
    # 准备后删除：旧 a2/0 -> 新 0 墓碑，旧 c2/2 -> 新 5 墓碑，存活行 pos 不压缩。
    # 共享服务器上此前的 alpha 剩余 3 行、~j 的 1 行仍存在；
    # ASCII 序：大写 < 小写 < '~'(0x7E)，故 ~j、~j1 排在最后。
    expect = [
        {"file": "alpha", "pos": 2, "value": -(2**63)},
        {"file": "alpha", "pos": 3, "value": 2**63 - 1},
        {"file": "alpha", "pos": 4, "value": 7},
        {"file": "b2", "pos": 0, "value": 40},
        {"file": "~j", "pos": 1, "value": 7},
        {"file": "~j1", "pos": 1, "value": 20},
        {"file": "~j1", "pos": 2, "value": 30},
        {"file": "~j1", "pos": 3, "value": 50},
        {"file": "~j1", "pos": 4, "value": 60},
        {"file": "~j1", "pos": 6, "value": 80},
    ]
    check(rows_of(body) == expect,
          f"j1 发布：交错删除已映射至新位置（实际 {rows_of(body)}）")

    # j2 的输入 c2 已被 j1 替换 -> 409，且不推进版本、不改变发布状态
    code, conflict = srv.post("/publish", {"job": "j2"})
    check(code == 409, f"竞争输入被替换 -> 409（实际 {code} {conflict}）")
    check(srv.post("/read", {"version": v_pub1})[1] == body, "409 后状态完全不变")
    # 再试一次仍是 409
    check(srv.post("/publish", {"job": "j2"})[0] == 409, "冲突可重复检测")

    # 无关文件 b2 的变更不阻止新的压实
    _, d2 = srv.post("/delete", {"positions": [["b2", 0]]})
    _, prep3 = srv.post("/prepare", {"job": "j3", "files": ["b2"]})
    check(prep3["base"] == d2["version"], "无关变更后仍可准备新作业")
    _, pub3 = srv.post("/publish", {"job": "j3"})
    body3 = srv.post("/read", {"version": pub3["version"]})[1]
    # b2 唯一行已删 -> ~j3 为空文件，不产生行
    check(all(r["file"] != "b2" for r in rows_of(body3)) and
          all(not (r["file"] == "~j3") for r in rows_of(body3)),
          "全删文件压实为空输出，读取无行")

    # 历史版本：准备 base 时刻快照仍完整。
    # base=10：alpha 剩 3 行、beta 空、a 已在 v7 被 ~j 替换（~j 1 行）、
    # a2 3 行、b2 1 行、c2 4 行 -> 12 行
    hist = srv.post("/read", {"version": prep1["base"]})[1]
    check(len(hist["rows"]) == 12,
          f"历史 base 版本永久完整可读（12 行，实际 {len(hist['rows'])} 行）")
    check([r["file"] for r in hist["rows"]] == sorted(r["file"] for r in hist["rows"]),
          "历史快照同样按文件 ASCII 序")


# ---------------------------------------------------------------- 9. 准备前删除/null
def test_pre_prepare_delete_and_nulls() -> None:
    section("9. 边界：准备前已删除行、真实 null 与占位 null、单文件多作业")
    data = tempfile.mkdtemp(prefix="lakecomp_edge_")
    try:
        srv = Server(data)
        # n: [null(真实), 11, 22, 33]，准备前先删 pos=1（值 11）
        srv.post("/append", {"file": "n", "values": [None, 11, 22, 33]})
        srv.post("/delete", {"positions": [["n", 1]]})
        _, prep = srv.post("/prepare", {"job": "e1", "files": ["n"]})
        check(prep == {"base": 2}, f"准备固定 base=2（实际 {prep}）")
        # 准备之后再删 pos=2（值 22），发布时应映射到同一新坐标 2
        srv.post("/delete", {"positions": [["n", 2]]})
        _, pub = srv.post("/publish", {"job": "e1"})
        body = srv.post("/read", {"version": pub["version"]})[1]
        # 输出槽位 0..3：0=真实 null 存活、1=准备前死行占位、2=准备后死行、3=33
        check(rows_of(body) == [
            {"file": "~e1", "pos": 0, "value": None},
            {"file": "~e1", "pos": 3, "value": 33},
        ], f"真实 null 保留、两类死行均墓碑化且坐标不压缩（实际 {rows_of(body)}）")

        # 直接检查 Parquet：准备前死行（pos1）为 null 占位；
        # 准备后才删的 pos2 在私有文件中仍保留原值 22，发布时由墓碑屏蔽。
        import pyarrow.parquet as pq
        tbl = pq.read_table(Path(data) / "parquet" / "~e1.parquet")
        check(tbl.column("value").to_pylist() == [None, None, 22, 33],
              f"私有 Parquet 槽位：真实 null/占位 null/22/33（实际 {tbl.column('value').to_pylist()}）")

        # 版本序列：append v1、准备前 delete v2、prepare 不增版、
        # 准备后 delete v3、publish v4
        check(pub["version"] == 4, f"版本序列 1/2/准备不变/3/4（实际 {pub}）")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


# ---------------------------------------------------------------- 10. 链式压实
def test_chained_compaction() -> None:
    section("10. 链式压实：对 ~job 输出再次删除与压实，墓碑跨轮翻译")
    data = tempfile.mkdtemp(prefix="lakecomp_chain_")
    try:
        srv = Server(data)
        srv.post("/append", {"file": "s1", "values": [1, 2, 3]})
        srv.post("/append", {"file": "s2", "values": [4, 5]})
        srv.post("/prepare", {"job": "j1", "files": ["s1", "s2"]})
        _, pub1 = srv.post("/publish", {"job": "j1"})
        check(pub1["version"] == 3, f"首轮发布 v3（实际 {pub1}）")
        # 对压实输出文件直接按新位置删除
        _, d = srv.post("/delete", {"positions": [["~j1", 2]]})
        check(d["version"] == 4, "对 ~j1 的删除增版本至 4")
        # 再次压实同一个输出文件
        _, prep2 = srv.post("/prepare", {"job": "j2", "files": ["~j1"]})
        check(prep2 == {"base": 4}, f"二轮准备固定 base=4（实际 {prep2}）")
        _, pub2 = srv.post("/publish", {"job": "j2"})
        check(pub2["version"] == 5, f"二轮发布 v5（实际 {pub2}）")
        body = srv2_rows = srv.post("/read", {"version": 5})[1]
        check(rows_of(body) == [
            {"file": "~j2", "pos": 0, "value": 1},
            {"file": "~j2", "pos": 1, "value": 2},
            {"file": "~j2", "pos": 3, "value": 4},
            {"file": "~j2", "pos": 4, "value": 5},
        ], f"二轮压实后 pos2 墓碑跨轮保持（实际 {rows_of(body)}）")
        # 历史各版本永久可读
        check(len(srv.post("/read", {"version": 3})[1]["rows"]) == 5,
              "v3 首轮压实结果 5 行可回溯")
        check(len(srv.post("/read", {"version": 4})[1]["rows"]) == 4,
              "v4 删除后 4 行可回溯")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


# ---------------------------------------------------------------- 6. 竞争压实
def test_competing_compaction() -> None:
    section("6. 竞争压实：重叠输入串行发布等价")
    data = tempfile.mkdtemp(prefix="lakecomp_compete_")
    try:
        srv = Server(data)
        srv.post("/append", {"file": "x", "values": [1, 2]})
        srv.post("/append", {"file": "y", "values": [3, 4]})
        _, p1 = srv.post("/prepare", {"job": "k1", "files": ["x", "y"]})
        _, p2 = srv.post("/prepare", {"job": "k2", "files": ["x"]})
        _, pub1 = srv.post("/publish", {"job": "k1"})
        code, body = srv.post("/publish", {"job": "k2"})
        check(code == 409, f"后发布者输入已替换 -> 409（实际 {code} {body}）")
        cur = srv.post("/read", {"version": pub1["version"]})[1]
        check(rows_of(cur) == [
            {"file": "~k1", "pos": 0, "value": 1},
            {"file": "~k1", "pos": 1, "value": 2},
            {"file": "~k1", "pos": 2, "value": 3},
            {"file": "~k1", "pos": 3, "value": 4},
        ], "先发布者结果完整，失败者无副作用")
        # 失败作业保持 prepared，重试仍 409
        check(srv.post("/publish", {"job": "k2"})[0] == 409, "失败作业重试仍 409")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


# ---------------------------------------------------------------- 7. 重启/强退
def test_restart_crash() -> None:
    section("7. 重启与强退恢复")

    # 7a. 正常 SIGTERM 重启，全状态保持
    data = tempfile.mkdtemp(prefix="lakecomp_restart_")
    try:
        srv = Server(data)
        srv.post("/append", {"file": "r1", "values": [1, None, 3]})
        srv.post("/append", {"file": "r2", "values": [4, 5]})
        srv.post("/delete", {"positions": [["r1", 1]]})
        ver_before = srv.post("/prepare", {"job": "rj", "files": ["r1", "r2"]})[1]["base"]
        snap_before = srv.post("/read", {"version": ver_before})[1]
        srv.stop()

        srv2 = Server(data)
        snap_after = srv2.post("/read", {"version": ver_before})[1]
        check(snap_after == snap_before, "SIGTERM 重启后历史快照逐行一致")
        _, pub = srv2.post("/publish", {"job": "rj"})
        body = srv2.post("/read", {"version": pub["version"]})[1]
        check(rows_of(body) == [
            {"file": "~rj", "pos": 0, "value": 1},
            # pos 1 是 r1 已删除行的 null 占位槽，被墓碑化不读出
            {"file": "~rj", "pos": 2, "value": 3},
            {"file": "~rj", "pos": 3, "value": 4},
            {"file": "~rj", "pos": 4, "value": 5},
        ], "重启后准备的作业仍可发布，映射持久（死行占位不压缩坐标）")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)

    # 7b. 准备：私有文件写盘后、事务提交前强退 -> 完整旧版（无作业、无新文件登记）
    data = tempfile.mkdtemp(prefix="lakecomp_crash_prep_")
    try:
        srv = Server(data, crash="prepare_write")
        srv.post("/append", {"file": "p1", "values": [9, 8]})
        srv.post_expect_crash("/prepare", {"job": "pj", "files": ["p1"]})
        srv.hard_kill()
        time.sleep(0.3)
        srv2 = Server(data)  # 不带故障注入
        check(srv2.post("/read", {"version": 1})[1]["rows"] == [
            {"file": "p1", "pos": 0, "value": 9},
            {"file": "p1", "pos": 1, "value": 8},
        ], "准备期强退后恢复为完整旧版（版本 1）")
        code, b = srv2.post("/publish", {"job": "pj"})
        check(code == 400, "未提交的作业不存在（映射未落库）")
        parquet = sorted(os.listdir(Path(data) / "parquet"))
        check(parquet == ["p1.parquet"], f"崩溃残留临时/孤儿文件已清理（实际 {parquet}）")
        code, b = srv2.post("/prepare", {"job": "pj", "files": ["p1"]})
        check(code == 200 and b == {"base": 1}, f"崩溃后可重新准备同名 job（实际 {b}）")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)

    # 7c. 发布：提交前强退 -> 完整旧版，重试为全新发布
    data = tempfile.mkdtemp(prefix="lakecomp_crash_pre_")
    try:
        srv = Server(data, crash="publish_before_commit")
        srv.post("/append", {"file": "q1", "values": [5, 6]})
        srv.post("/prepare", {"job": "qj", "files": ["q1"]})
        srv.post_expect_crash("/publish", {"job": "qj"})
        srv.hard_kill()
        time.sleep(0.3)
        srv2 = Server(data)
        body = srv2.post("/read", {"version": 1})[1]
        check(body == {"version": 1, "rows": [
            {"file": "q1", "pos": 0, "value": 5},
            {"file": "q1", "pos": 1, "value": 6},
        ]}, "提交前强退：恢复完整旧版，输入文件未替换")
        _, pub = srv2.post("/publish", {"job": "qj"})
        check(pub["version"] == 2, f"重启后重新发布成功 -> version 2（实际 {pub}）")
        check(rows_of(srv2.post("/read", {"version": 2})[1]) == [
            {"file": "~qj", "pos": 0, "value": 5},
            {"file": "~qj", "pos": 1, "value": 6},
        ], "重新发布结果正确")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)

    # 7d. 发布：提交后强退 -> 完整新版，重试幂等返回同一版本
    data = tempfile.mkdtemp(prefix="lakecomp_crash_post_")
    try:
        srv = Server(data, crash="publish_after_commit")
        srv.post("/append", {"file": "z1", "values": [2, 4]})
        srv.post("/delete", {"positions": [["z1", 0]]})
        srv.post("/prepare", {"job": "zj", "files": ["z1"]})
        srv.post_expect_crash("/publish", {"job": "zj"})
        srv.hard_kill()
        time.sleep(0.3)
        srv2 = Server(data)
        body = srv2.post("/read", {"version": 3})[1]
        check(rows_of(body) == [{"file": "~zj", "pos": 1, "value": 4}],
              "提交后强退：恢复完整新版（含删除映射）")
        _, retry = srv2.post("/publish", {"job": "zj"})
        check(retry == {"version": 3}, f"强退后发布重试幂等返回版本 3（实际 {retry}）")
        check(srv2.post("/read", {"version": 3})[1] == body, "幂等重试无任何变化")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


# ---------------------------------------------------------------- 8. 并发交错
def test_concurrency() -> None:
    section("8. 并发交错：多线程混合请求一致性")
    data = tempfile.mkdtemp(prefix="lakecomp_conc_")
    try:
        srv = Server(data)
        for i in range(5):
            srv.post("/append", {"file": f"f{i}", "values": list(range(10))})

        errors: list[str] = []
        versions: list[int] = []
        lock = threading.Lock()

        def worker(w: int) -> None:
            try:
                for k in range(10):
                    idx = (w + k) % 5
                    code, b = srv.post("/delete",
                                       {"positions": [[f"f{idx}", k % 10]]})
                    if code != 200:
                        with lock:
                            errors.append(f"worker{w} delete {b}")
                    else:
                        with lock:
                            versions.append(b["version"])
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(str(e))

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check(not errors, f"并发删除无错误（{errors[:3]}）")
        check(len(versions) == 40 and versions == sorted(versions),
              "40 次删除串行化，版本严格单调（无丢更新）")

        cur = versions[-1]
        body = srv.post("/read", {"version": cur})[1]
        # 每个文件 10 行，40 次删除（幂等可能重复）-> 每文件墓碑数 <=10，总行数可算
        total = len(body["rows"])
        check(10 <= total <= 50, f"并发后行数合理={total}")
        # 每个版本号都能读取出不可变快照
        ok = all(srv.post("/read", {"version": v})[0] == 200
                 for v in {min(versions), max(versions)})
        check(ok, "并发期间所有历史版本可读")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


# ---------------------------------------------------------------- 11. 擦除
def test_erase_validation() -> None:
    section("11. 擦除：请求校验（400，拒绝无副作用）")
    data = tempfile.mkdtemp(prefix="lakecomp_erase_val_")
    try:
        srv = Server(data)
        srv.post("/append", {"file": "a", "values": [7, 7]})
        srv.post("/delete", {"positions": [["a", 1]]})
        base = srv.post("/read", {"version": 2})[1]
        parquet_before = {
            p.name: p.read_bytes() for p in (Path(data) / "parquet").iterdir()
        }

        expect_error(srv, "/erase", None, 400, "坏 JSON 语法", raw=b"{not json")
        # 重复成员
        code, _ = srv.post("/erase", None,
                           raw=b'{"version":1,"version":1,"positions":[]}')
        check(code == 400, "擦除重复成员 -> 400")
        expect_error(srv, "/erase", {"version": 1, "positions": [], "x": 1}, 400,
                     "擦除未知成员")
        expect_error(srv, "/erase", {"positions": []}, 400, "缺少 version")
        expect_error(srv, "/erase", {"version": 1}, 400, "缺少 positions")
        expect_error(srv, "/erase", {"version": True, "positions": []}, 400,
                     "version 布尔拒绝")
        expect_error(srv, "/erase", {"version": "1", "positions": []}, 400,
                     "version 类型错误")
        expect_error(srv, "/erase", {"version": -1, "positions": []}, 400,
                     "version 负数")
        expect_error(srv, "/erase", {"version": 99, "positions": []}, 400,
                     "version 未达（未来版本）")
        expect_error(srv, "/erase", {"version": 1, "positions": "x"}, 400,
                     "positions 非数组")
        expect_error(srv, "/erase", {"version": 1, "positions": [["a"]]}, 400,
                     "位置项长度错误")
        expect_error(srv, "/erase", {"version": 1, "positions": [[1, 0]]}, 400,
                     "位置项 file 类型错误")
        expect_error(srv, "/erase", {"version": 1, "positions": [["a", True]]}, 400,
                     "位置项 pos 布尔拒绝")
        expect_error(srv, "/erase", {"version": 1, "positions": [["a", -1]]}, 400,
                     "位置项 pos 负数")
        expect_error(srv, "/erase", {"version": 1, "positions": [["nope", 0]]}, 400,
                     "引用不存在文件")
        expect_error(srv, "/erase", {"version": 1, "positions": [["a", 5]]}, 400,
                     "pos 越界")
        expect_error(srv, "/erase", {"version": 2, "positions": [["a", 1]]}, 400,
                     "在该版本已删除的坐标不可擦除")

        check(srv.post("/read", {"version": 2})[1] == base,
              "全部拒绝后读取状态不变")
        parquet_after = {
            p.name: p.read_bytes() for p in (Path(data) / "parquet").iterdir()
        }
        check(parquet_after == parquet_before, "全部拒绝后 Parquet 文件逐字节不变")
        # 版本未被推进
        check(srv.post("/read", {"version": 3})[0] == 400, "拒绝的擦除不增版本")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


def test_erase_spec_example() -> None:
    section("12. 擦除：规格示例 a=[7,7] 压实为 ~j 后擦除 [a,0]")
    data = tempfile.mkdtemp(prefix="lakecomp_erase_spec_")
    try:
        srv = Server(data)
        srv.post("/append", {"file": "a", "values": [7, 7]})       # v1
        srv.post("/prepare", {"job": "j", "files": ["a"]})          # base=1
        srv.post("/publish", {"job": "j"})                          # v2
        # 历史版本 1 可见 a；当前版本 2 可见 ~j
        code, body = srv.post("/erase", {"version": 1, "positions": [["a", 0]]})
        check(code == 200 and body == {"erased": 1},
              f"擦除 [a,0] -> {{'erased':1}}（实际 {code} {body}）")

        # 历史 a 原值被擦除，当前 ~j 的对应行也被擦除；同值其他行保持
        hist = srv.post("/read", {"version": 1})[1]
        check(hist["rows"] == [
            {"file": "a", "pos": 0, "value": None},
            {"file": "a", "pos": 1, "value": 7},
        ], f"历史 a 值为 [null,7]（实际 {hist['rows']}）")
        cur = srv.post("/read", {"version": 2})[1]
        check(cur["rows"] == [
            {"file": "~j", "pos": 0, "value": None},
            {"file": "~j", "pos": 1, "value": 7},
        ], f"当前 ~j 值为 [null,7]（实际 {cur['rows']}）")

        # 仅改写目标单元格：直接读 Parquet，同值其他行仍为 7
        import pyarrow.parquet as pq
        a_vals = pq.read_table(Path(data) / "parquet" / "a.parquet").column(
            "value").to_pylist()
        j_vals = pq.read_table(Path(data) / "parquet" / "~j.parquet").column(
            "value").to_pylist()
        check(a_vals == [None, 7], f"a.parquet 解码为 [None,7]（实际 {a_vals}）")
        check(j_vals == [None, 7], f"~j.parquet 解码为 [None,7]（实际 {j_vals}）")

        # 不增版本
        check(srv.post("/read", {"version": 3})[0] == 400, "擦除不增版本（当前仍为 2）")

        # 重试结果相同（幂等），且返回相同的 N
        code2, body2 = srv.post("/erase", {"version": 1, "positions": [["a", 0]]})
        check(code2 == 200 and body2 == {"erased": 1},
              f"重试返回相同 {{'erased':1}}（实际 {body2}）")
        check(srv.post("/read", {"version": 2})[1] == cur, "重试不改变状态")
        # 以当前坐标同样可擦除同一逻辑行（已按来源识别为同一行）
        code3, body3 = srv.post("/erase", {"version": 2, "positions": [["~j", 0]]})
        check(code3 == 200 and body3 == {"erased": 1},
              f"经 ~j 坐标擦除同一逻辑行 -> erased 1（实际 {body3}）")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


def test_erase_chained_and_pending() -> None:
    section("13. 擦除：链式压实、未发布副本、同值不同行")
    data = tempfile.mkdtemp(prefix="lakecomp_erase_chain_")
    try:
        srv = Server(data)
        # 两个文件同值，验证同值其他行不受影响
        srv.post("/append", {"file": "s1", "values": [7, 8]})       # v1
        srv.post("/append", {"file": "s2", "values": [7, 9]})       # v2
        srv.post("/prepare", {"job": "j1", "files": ["s1", "s2"]})  # 合并 [7,8,7,9]
        srv.post("/publish", {"job": "j1"})                          # v3
        # 对已发布输出再次准备一个未发布作业 j2（待定副本）
        srv.post("/prepare", {"job": "j2", "files": ["~j1"]})        # 不增版本
        # 链式第二轮也已发布的情形用 j3 覆盖
        srv.post("/prepare", {"job": "j3", "files": ["~j1"]})
        # 先发布 j3 -> v4，再准备 j2（~j1 已被 j3 替换会 409，故 j2 在发布前已准备）
        # 说明：j2 保持 prepared 未发布，作为“待定发布副本”用例。

        # 擦除逻辑行：s1 pos0（值7）通过 s1 -> ~j1 pos0 -> {~j2 pos0, ~j3 pos0} 传播
        code, body = srv.post("/erase", {"version": 1, "positions": [["s1", 0]]})
        check(code == 200 and body == {"erased": 1},
              f"擦除 s1/0 一个逻辑行 -> erased 1（实际 {body}）")

        import pyarrow.parquet as pq
        def vals(name):
            return pq.read_table(Path(data) / "parquet" / name).column(
                "value").to_pylist()
        check(vals("s1.parquet") == [None, 8], f"s1 原值擦除（实际 {vals('s1.parquet')}）")
        check(vals("~j1.parquet") == [None, 8, 7, 9],
              f"~j1 仅目标槽置 null，同值 7 在 pos2 保留（实际 {vals('~j1.parquet')}）")
        check(vals("~j2.parquet") == [None, 8, 7, 9],
              f"未发布的 ~j2 副本同步擦除（实际 {vals('~j2.parquet')}）")
        check(vals("~j3.parquet") == [None, 8, 7, 9],
              f"另一待定副本 ~j3 同步擦除（实际 {vals('~j3.parquet')}）")

        # 发布待定作业：原值不会复活（该行值已是 null），同值其他行保留
        _, pub2 = srv.post("/publish", {"job": "j2"})
        rows2 = srv.post("/read", {"version": pub2["version"]})[1]["rows"]
        j2 = [r for r in rows2 if r["file"] == "~j2"]
        check(j2 == [
            {"file": "~j2", "pos": 0, "value": None},
            {"file": "~j2", "pos": 1, "value": 8},
            {"file": "~j2", "pos": 2, "value": 7},
            {"file": "~j2", "pos": 3, "value": 9},
        ], f"发布 ~j2 后目标行为 null（不复活），同值 pos2=7 保留（实际 {j2}）")

        # 历史版本仍可读，且历史中被擦除行确为 null
        h1 = srv.post("/read", {"version": 1})[1]["rows"]
        check(h1 == [{"file": "s1", "pos": 0, "value": None},
                     {"file": "s1", "pos": 1, "value": 8}],
              f"历史 v1 的 s1 已被擦除（实际 {h1}）")
        h2 = srv.post("/read", {"version": 2})[1]["rows"]
        check(h2 == [{"file": "s1", "pos": 0, "value": None},
                     {"file": "s1", "pos": 1, "value": 8},
                     {"file": "s2", "pos": 0, "value": 7},
                     {"file": "s2", "pos": 1, "value": 9}],
              f"同值其他文件 s2 不受影响（实际 {h2}）")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


def test_erase_multi_and_empty() -> None:
    section("14. 擦除：多坐标去重、空数组合法、跨文件同源合并")
    data = tempfile.mkdtemp(prefix="lakecomp_erase_multi_")
    try:
        srv = Server(data)
        srv.post("/append", {"file": "p", "values": [1, 2]})   # v1
        srv.post("/append", {"file": "q", "values": [3, 4]})   # v2
        srv.post("/prepare", {"job": "m", "files": ["p", "q"]})  # ~m = [1,2,3,4]
        srv.post("/publish", {"job": "m"})                       # v3

        # 空数组合法：返回 erased 0，不增版本
        code, body = srv.post("/erase", {"version": 3, "positions": []})
        check(code == 200 and body == {"erased": 0},
              f"空 positions 合法 -> {{'erased':0}}（实际 {code} {body}）")
        check(srv.post("/read", {"version": 4})[0] == 400, "空擦除不增版本")

        # 重复坐标合并；跨文件同源坐标（p/0 与 ~m/0）也合并为同一逻辑行
        code, body = srv.post("/erase", {
            "version": 1,
            "positions": [["p", 0], ["p", 0]],
        })
        check(code == 200 and body == {"erased": 1},
              f"重复坐标合并 -> erased 1（实际 {body}）")
        code, body = srv.post("/erase", {
            "version": 2,
            "positions": [["p", 1], ["q", 0]],
        })
        check(code == 200 and body == {"erased": 2},
              f"两个不同逻辑行 -> erased 2（实际 {body}）")
        # 跨版本不可见坐标 -> 400
        code, body = srv.post("/erase", {
            "version": 1,
            "positions": [["p", 0], ["~m", 0]],
        })
        # 版本 1 中 ~m 尚不存在 -> 引用错误 400
        check(code == 400, f"跨版本不可见坐标 -> 400（实际 {code} {body}）")
        code, body = srv.post("/erase", {
            "version": 3,
            "positions": [["p", 0], ["~m", 0]],
        })
        # 版本 3 中 p 已被替换 -> 引用错误 400
        check(code == 400, f"已被替换文件在 v3 不可见 -> 400（实际 {code} {body}）")
        # 已在 v2 擦除 p/1 与 q/0，相应的 ~m 槽位（2、3）已为 null；
        # 再对 v3 的 ~m/0、~m/1 擦除，去重后为 2 行
        code, body = srv.post("/erase", {
            "version": 3, "positions": [["~m", 0], ["~m", 0], ["~m", 1]],
        })
        check(code == 200 and body == {"erased": 2},
              f"当前压实文件多坐标去重 -> erased 2（实际 {body}）")

        import pyarrow.parquet as pq
        m = pq.read_table(Path(data) / "parquet" / "~m.parquet").column(
            "value").to_pylist()
        check(m == [None, None, None, 4],
              f"~m 前两槽为本次擦除，pos2 为 v2 已擦除的 p/1（实际 {m}）")
        # 擦除只把值置 null，不改变可见性：被擦除的行仍以 null 返回
        mrows = srv.post("/read", {"version": 3})[1]["rows"]
        check(mrows == [
            {"file": "~m", "pos": 0, "value": None},
            {"file": "~m", "pos": 1, "value": None},
            {"file": "~m", "pos": 2, "value": None},
            {"file": "~m", "pos": 3, "value": 4},
        ], f"被擦除行仍可见且为 null（pos 2 亦在 v2 已擦除）（实际 {mrows}）")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


def test_erase_crash_recovery() -> None:
    section("15. 擦除：强退恢复（意图前 / 意图后 / 逐文件改写中 / 改写后置done前）")

    # 15a. 擦除意图持久化前强退 -> 原态完整保留（未擦除），重启可重新擦除
    data = tempfile.mkdtemp(prefix="lakecomp_erase_preintent_")
    try:
        srv = Server(data, crash="erase_before_intent")
        srv.post("/append", {"file": "u", "values": [4, 4]})
        srv.post("/prepare", {"job": "uj", "files": ["u"]})
        srv.post("/publish", {"job": "uj"})
        srv.post_expect_crash("/erase", {"version": 1, "positions": [["u", 0]]})
        srv.hard_kill()
        time.sleep(0.3)
        srv2 = Server(data)
        import pyarrow.parquet as pq
        for name in ("u.parquet", "~uj.parquet"):
            v = pq.read_table(Path(data) / "parquet" / name).column(
                "value").to_pylist()
            check(v == [4, 4], f"意图前强退：{name} 原值保留（实际 {v}）")
        # 原态保留，之后可正常完成擦除
        _, body = srv2.post("/erase", {"version": 1, "positions": [["u", 0]]})
        check(body == {"erased": 1}, f"重启后可完成擦除（实际 {body}）")
        check(pq.read_table(Path(data) / "parquet" / "u.parquet").column(
            "value").to_pylist() == [None, 4], "延迟擦除结果正确")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)

    # 15b. 意图提交后、改写前强退：重启必须先补完擦除才服务
    data = tempfile.mkdtemp(prefix="lakecomp_erase_crash_")
    try:
        srv = Server(data, crash="erase_after_intent")
        srv.post("/append", {"file": "e", "values": [5, 5]})
        srv.post("/prepare", {"job": "ej", "files": ["e"]})
        srv.post("/publish", {"job": "ej"})
        srv.post_expect_crash("/erase", {"version": 1, "positions": [["e", 0]]})
        srv.hard_kill()
        time.sleep(0.3)
        # 重启：必须先补完擦除才服务
        srv2 = Server(data)
        hist = srv2.post("/read", {"version": 1})[1]["rows"]
        check(hist == [{"file": "e", "pos": 0, "value": None},
                       {"file": "e", "pos": 1, "value": 5}],
              f"意图后强退：重启已完成擦除（历史 e 值 {hist}）")
        cur = srv2.post("/read", {"version": 2})[1]["rows"]
        check(cur == [{"file": "~ej", "pos": 0, "value": None},
                      {"file": "~ej", "pos": 1, "value": 5}],
              f"意图后强退：当前 ~ej 也已擦除（实际 {cur}）")
        import pyarrow.parquet as pq
        for name in ("e.parquet", "~ej.parquet"):
            v = pq.read_table(Path(data) / "parquet" / name).column(
                "value").to_pylist()
            check(v == [None, 5], f"{name} 解码 [None,5]（实际 {v}）")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)

    # 15b. 逐文件改写中强退（多文件逐个改写，中途退出）：重启补完
    data = tempfile.mkdtemp(prefix="lakecomp_erase_mid_")
    try:
        srv = Server(data, crash="erase_mid_rewrite")
        srv.post("/append", {"file": "f1", "values": [1, 2]})   # v1
        srv.post("/prepare", {"job": "tmpj", "files": ["f1"]})  # 仅占位，不发布
        srv.post("/append", {"file": "f2", "values": [3, 4]})   # v2
        srv.post("/prepare", {"job": "mj", "files": ["f1", "f2"]})
        srv.post("/publish", {"job": "mj"})                     # v3；~mj 继承两输入
        # f1、f2 在 v2 同时可见（~mj 尚未发布，但仍作为后代副本被擦除）
        srv.post_expect_crash("/erase", {
            "version": 2,
            "positions": [["f1", 0], ["f2", 0]],
        })
        srv.hard_kill()
        time.sleep(0.3)
        srv2 = Server(data)
        import pyarrow.parquet as pq
        f1 = pq.read_table(Path(data) / "parquet" / "f1.parquet").column(
            "value").to_pylist()
        f2 = pq.read_table(Path(data) / "parquet" / "f2.parquet").column(
            "value").to_pylist()
        mj = pq.read_table(Path(data) / "parquet" / "~mj.parquet").column(
            "value").to_pylist()
        check(f1 == [None, 2] and f2 == [None, 4],
              f"改写中强退：两输入端均已擦除（f1={f1} f2={f2}）")
        check(mj == [None, 2, None, 4],
              f"改写中强退：压实输出同步擦除（~mj={mj}）")
        # 恢复后重试返回相同 N
        _, body = srv2.post("/erase", {"version": 2, "positions": [["f1", 0]]})
        check(body == {"erased": 1}, f"恢复后重试 erased 1（实际 {body}）")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)

    # 15c. 改写完成、置 done 前强退：重启补完标记，结果不变
    data = tempfile.mkdtemp(prefix="lakecomp_erase_post_")
    try:
        srv = Server(data, crash="erase_after_rewrites")
        srv.post("/append", {"file": "g", "values": [6, 6, 6]})
        srv.post_expect_crash("/erase", {"version": 1, "positions": [["g", 1]]})
        srv.hard_kill()
        time.sleep(0.3)
        srv2 = Server(data)
        import pyarrow.parquet as pq
        g = pq.read_table(Path(data) / "parquet" / "g.parquet").column(
            "value").to_pylist()
        check(g == [6, None, 6], f"改写后强退：仅目标槽 null（实际 {g}）")
        rows = srv2.post("/read", {"version": 1})[1]["rows"]
        check(rows == [
            {"file": "g", "pos": 0, "value": 6},
            {"file": "g", "pos": 1, "value": None},
            {"file": "g", "pos": 2, "value": 6},
        ], f"读取值正确且坐标不变（实际 {rows}）")
        _, body = srv2.post("/erase", {"version": 1, "positions": [["g", 1]]})
        check(body == {"erased": 1}, "恢复后重试幂等")
        srv2.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


def test_erase_interleaved_and_future() -> None:
    section("17. 擦除：交错删除后擦除 + 擦除后的新压实不复活原值")
    data = tempfile.mkdtemp(prefix="lakecomp_erase_inter_")
    try:
        import pyarrow.parquet as pq
        srv = Server(data)
        srv.post("/append", {"file": "h", "values": [7, 7, 9]})    # v1
        srv.post("/delete", {"positions": [["h", 0]]})             # v2（pos0 死）
        srv.post("/prepare", {"job": "hj", "files": ["h"]})        # ~hj=[null,7,9]
        srv.post("/publish", {"job": "hj"})                        # v3

        # 以版本 1 擦除 [h,0]：该坐标在 v1 可见（删除发生于 v2）
        code, body = srv.post("/erase", {"version": 1, "positions": [["h", 0]]})
        check(code == 200 and body == {"erased": 1},
              f"交错删除前历史坐标可擦除 -> erased 1（实际 {code} {body}）")
        # 历史 h、当前 ~hj 的目标行均置 null；其余坐标不变
        h = pq.read_table(Path(data) / "parquet" / "h.parquet").column(
            "value").to_pylist()
        hj = pq.read_table(Path(data) / "parquet" / "~hj.parquet").column(
            "value").to_pylist()
        check(h == [None, 7, 9], f"h 原值擦除、同值 pos1 保留（实际 {h}）")
        check(hj == [None, 7, 9], f"~hj 目标槽 null、其余保留（实际 {hj}）")
        # 删除可见性保持：v1 两行均可见（pos0 为 null），v3 仅 pos1/2
        v1 = srv.post("/read", {"version": 1})[1]["rows"]
        check(v1 == [
            {"file": "h", "pos": 0, "value": None},
            {"file": "h", "pos": 1, "value": 7},
            {"file": "h", "pos": 2, "value": 9},
        ], f"v1 删除可见性不变（pos0 仍可见为 null）（实际 {v1}）")
        v3 = srv.post("/read", {"version": 3})[1]["rows"]
        check(v3 == [
            {"file": "~hj", "pos": 1, "value": 7},
            {"file": "~hj", "pos": 2, "value": 9},
        ], f"v3 已删除的 pos0 仍不可见（实际 {v3}）")

        # 擦除后进行新一轮压实：新输出必须继承 null，绝不复活原值
        srv.post("/prepare", {"job": "hk", "files": ["~hj"]})      # 空操作不增版
        _, pub = srv.post("/publish", {"job": "hk"})
        v4 = srv.post("/read", {"version": pub["version"]})[1]["rows"]
        check(v4 == [
            {"file": "~hk", "pos": 1, "value": 7},
            {"file": "~hk", "pos": 2, "value": 9},
        ], f"擦除后压实继承 null（pos0 死且原值不复活）（实际 {v4}）")
        hk = pq.read_table(Path(data) / "parquet" / "~hk.parquet").column(
            "value").to_pylist()
        check(hk == [None, 7, 9], f"新输出 ~hk 物理值 [None,7,9]（实际 {hk}）")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


def test_erase_concurrency() -> None:
    section("16. 擦除：与写/读并发时串行等价")
    data = tempfile.mkdtemp(prefix="lakecomp_erase_conc_")
    try:
        srv = Server(data)
        srv.post("/append", {"file": "c", "values": list(range(20))})
        srv.post("/prepare", {"job": "cj", "files": ["c"]})
        srv.post("/publish", {"job": "cj"})

        errors: list[str] = []
        lock = threading.Lock()

        def eraser():
            try:
                for k in range(0, 20, 2):
                    code, b = srv.post("/erase", {"version": 2, "positions": [["~cj", k]]})
                    if code != 200:
                        with lock:
                            errors.append(f"erase {k}: {b}")
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(str(e))

        def reader():
            try:
                for _ in range(20):
                    code, b = srv.post("/read", {"version": 2})
                    if code != 200:
                        with lock:
                            errors.append(f"read: {b}")
                        continue
                    # 读到的快照形状必须始终完整（20 行，坐标连续）
                    if [r["pos"] for r in b["rows"]] != list(range(20)):
                        with lock:
                            errors.append("读取到形状不完整的中间态")
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(str(e))

        threads = [threading.Thread(target=eraser), threading.Thread(target=reader),
                   threading.Thread(target=reader)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check(not errors, f"擦除与读并发无错误、无中间态（{errors[:3]}）")

        import pyarrow.parquet as pq
        c = pq.read_table(Path(data) / "parquet" / "~cj.parquet").column(
            "value").to_pylist()
        check(c == [None if i % 2 == 0 else i for i in range(20)],
              f"偶数槽全部擦除、奇数槽保留（实际 {c}）")
        check(srv.post("/read", {"version": 3})[0] == 400, "并发擦除不增版本")
        srv.stop()
    finally:
        shutil.rmtree(data, ignore_errors=True)


def main() -> int:
    base_data = tempfile.mkdtemp(prefix="lakecomp_main_")
    try:
        srv = Server(base_data)
        test_validation(srv)
        test_append_read(srv)
        test_delete(srv)
        test_spec_example(srv)
        test_interleaved_delete(srv)
        srv.stop()
        test_competing_compaction()
        test_restart_crash()
        test_concurrency()
        test_pre_prepare_delete_and_nulls()
        test_chained_compaction()
        test_erase_validation()
        test_erase_spec_example()
        test_erase_chained_and_pending()
        test_erase_multi_and_empty()
        test_erase_crash_recovery()
        test_erase_interleaved_and_future()
        test_erase_concurrency()
    finally:
        shutil.rmtree(base_data, ignore_errors=True)

    print(f"\n{'='*60}")
    print(f"通过 {PASS} 项，失败 {FAIL} 项")
    if FAILURES:
        print("失败项：")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("全部验收通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
