"""湖表压实 HTTP 后端（SQLite + PyArrow，零第三方 Web 框架）。

================================================================================
语义摘要
================================================================================
* 初态：版本 0，空表。
* 追加  POST /append   {file, values}：file/job 为非空 ASCII 字母数字串且永不复用；
  values 为 int64 或 null 数组（整数禁止布尔），按追加顺序以零基 pos 存 Parquet。
* 删除  POST /delete   {positions}：每项 [file, pos]，须引用当前文件界内行；
  重复坐标、已删除坐标允许；空数组亦成功并递增版本。
* 准备  POST /prepare  {job, files}：固定此刻版本为 base；按文件 ASCII 序、原行号
  将未删除行写入私有文件 "~" + job；持久保存 (旧坐标 -> 新坐标) 位置映射；不增版本。
* 发布  POST /publish  {job}：任一输入文件已被替换 -> 409；否则仅替换这些文件，
  准备之后对旧坐标的删除映射至新位置，递增版本；同 job 发布重试幂等返回原版本。
* 读取  POST /read     {version}：返回该版本完整快照，按文件 ASCII 序、行号排序，
  保留重复值与 null；历史版本永久可读（文件与墓碑均不回收）。
* 非法 JSON / 未知或重复成员 / 类型错误 / 引用错误 -> 400，报任一 JSON 路径；
  标识（file/job）复用 -> 409；被拒绝的请求不改变任何状态。
* 所有落盘遵循“文件及映射先落盘、SQLite 单事务原子提交”，强退前后恢复为完整旧版/新版。

存储布局（数据目录可通过 --data 指定）：
    data/meta.sqlite        元数据、墓碑、作业、位置映射（WAL + synchronous=FULL）
    data/parquet/<id>.parquet   原始文件与压实输出文件，均为单列 int64 Parquet
    data/parquet/*.tmp          崩溃残留的临时文件，启动时清理
"""

from __future__ import annotations

import json
import os
import re
import signal
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
SCHEMA = pa.schema([pa.field("value", pa.int64(), nullable=True)])
MAX_BODY_BYTES = int(os.environ.get("LAKECOMP_MAX_BODY", str(256 * 1024 * 1024)))


class ApiError(Exception):
    """业务异常：status 为 HTTP 状态码，path 为出错 JSON 路径（零基下标）。"""

    def __init__(self, status: int, message: str, path: str = "$"):
        super().__init__(message)
        self.status = status
        self.message = message
        self.path = path


_ASCII_ALNUM_RE = re.compile(r"[A-Za-z0-9]+$")


def _is_ascii_alnum(s: str) -> bool:
    return bool(_ASCII_ALNUM_RE.fullmatch(s))


def _member_path(parent: str, key: str) -> str:
    """构造对象成员的 JSONPath：普通标识用点号，其余用方括号。"""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        return f"{parent}.{key}" if parent != "$" else f"$.{key}"
    return f"{parent}[{json.dumps(key, ensure_ascii=False)}]"


def _index_path(parent: str, i: int) -> str:
    return f"{parent}[{i}]"


def _find_duplicate_member_path(text: str) -> Optional[str]:
    """在语法合法的 JSON 文本上找出第一个重复对象成员的路径；无则返回 None。

    递归下降扫描，只跟踪对象键所在的路径（数组下标同样记录）。
    """
    decoder = json.JSONDecoder()
    n = len(text)

    def skip_ws(i: int) -> int:
        while i < n and text[i] in " \t\r\n":
            i += 1
        return i

    def walk(i: int, path: str) -> tuple[Any, int]:
        i = skip_ws(i)
        ch = text[i]
        if ch == "{":
            obj: dict[str, Any] = {}
            i += 1
            i = skip_ws(i)
            if i < n and text[i] == "}":
                return obj, i + 1
            while True:
                i = skip_ws(i)
                key, j = decoder.raw_decode(text, i)
                member_path = _member_path(path, key)
                if key in obj:
                    raise _DuplicateKey(member_path)
                j = skip_ws(j)
                assert text[j] == ":"
                j = skip_ws(j + 1)
                value, j = walk(j, member_path)
                obj[key] = value
                j = skip_ws(j)
                if text[j] == ",":
                    i = j + 1
                    continue
                if text[j] == "}":
                    return obj, j + 1
                raise AssertionError("unreachable: 语法已先行校验")
        if ch == "[":
            arr: list[Any] = []
            i += 1
            i = skip_ws(i)
            if i < n and text[i] == "]":
                return arr, i + 1
            idx = 0
            while True:
                value, i = walk(i, _index_path(path, idx))
                arr.append(value)
                idx += 1
                i = skip_ws(i)
                if text[i] == ",":
                    i += 1
                    continue
                if text[i] == "]":
                    return arr, i + 1
                raise AssertionError("unreachable: 语法已先行校验")
        value, j = decoder.raw_decode(text, i)
        return value, j

    class _DuplicateKey(Exception):
        def __init__(self, path: str):
            self.path = path

    try:
        walk(0, "$")
    except _DuplicateKey as exc:
        return exc.path
    return None


def parse_request_body(raw: bytes) -> dict[str, Any]:
    """解析请求体：语法/重复成员/顶层类型错误一律 400。"""
    if not raw:
        raise ApiError(400, "请求体为空，期望 JSON 对象", "$")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ApiError(400, "请求体不是合法 UTF-8", "$")
    try:
        # 严格常量：拒绝 NaN / Infinity
        obj = json.loads(text, parse_constant=lambda c: (_ for _ in ()).throw(
            ApiError(400, f"非法 JSON 常量 {c}", "$")))
    except ApiError:
        raise
    except json.JSONDecodeError:
        # 语法错误统一报根路径 $
        raise ApiError(400, "非法 JSON 语法", "$")
    dup = _find_duplicate_member_path(text)
    if dup is not None:
        raise ApiError(400, "重复的 JSON 对象成员", dup)
    if not isinstance(obj, dict):
        raise ApiError(400, "请求体必须是 JSON 对象", "$")
    return obj


def _require_members(obj: dict[str, Any], allowed: set[str], where: str = "$") -> None:
    unknown = set(obj) - allowed
    if unknown:
        key = sorted(unknown)[0]
        raise ApiError(400, f"未知成员 {key!r}", _member_path(where, key))


class Store:
    """SQLite 元数据 + Parquet 数据文件。所有写操作经全局锁串行化。"""

    def __init__(self, data_dir: str | os.PathLike[str]):
        self.data_dir = Path(data_dir).resolve()
        self.parquet_dir = self.data_dir / "parquet"
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "meta.sqlite"
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._init_schema()
        self._cleanup_orphan_files()
        # 故障注入（仅用于验收强退恢复）：取值为
        #   prepare_write / publish_before_commit / publish_after_commit
        # 一次性：触发后立即从环境移除，避免重启后的新进程在同阶段再次崩溃。
        self.crash_points: set[str] = set()
        crash = os.environ.get("LAKECOMP_CRASH", "")
        if crash:
            for point in crash.split(","):
                point = point.strip()
                if point:
                    self.crash_points.add(point)

    def _crash(self, point: str) -> None:
        if point in self.crash_points:
            self.crash_points.discard(point)
            os.environ.pop("LAKECOMP_CRASH", None)
            os._exit(137)

    # ------------------------------------------------------------------ 连接
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.db_path, timeout=30.0, isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    def _init_schema(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                current_version INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO meta (id, current_version) VALUES (1, 0);

            -- 文件登记：原始追加文件与压实输出（~job）统一在此。
            -- replaced_at 非空表示已被某作业发布替换，读取时跳过。
            CREATE TABLE IF NOT EXISTS files (
                id            TEXT PRIMARY KEY,
                nrows         INTEGER NOT NULL,
                created_ver   INTEGER NOT NULL,
                replaced_at   INTEGER,
                replaced_job  TEXT
            );

            -- 行级墓碑：坐标 (file, pos) 对当前文件不可变，一次删除永久有效。
            CREATE TABLE IF NOT EXISTS tombstones (
                file TEXT NOT NULL,
                pos  INTEGER NOT NULL,
                del_ver INTEGER NOT NULL,
                PRIMARY KEY (file, pos)
            ) WITHOUT ROWID;

            -- 压实作业生命周期：prepared -> published。
            CREATE TABLE IF NOT EXISTS jobs (
                job           TEXT PRIMARY KEY,
                state         TEXT NOT NULL CHECK (state IN ('prepared', 'published')),
                base_version  INTEGER NOT NULL,
                created_ver   INTEGER NOT NULL,
                published_ver INTEGER
            );
            CREATE TABLE IF NOT EXISTS job_inputs (
                job       TEXT NOT NULL,
                file      TEXT NOT NULL,
                file_seq  INTEGER NOT NULL,           -- 准备时 ASCII 序下标
                PRIMARY KEY (job, file)
            );
            -- 持久位置映射：仅包含准备时刻未删除的行。
            CREATE TABLE IF NOT EXISTS job_mapping (
                job       TEXT NOT NULL,
                old_file  TEXT NOT NULL,
                old_pos   INTEGER NOT NULL,
                new_pos   INTEGER NOT NULL,
                PRIMARY KEY (job, old_file, old_pos)
            ) WITHOUT ROWID;
            """
        )

    def _cleanup_orphan_files(self) -> None:
        """删除未被任何已提交状态引用的 parquet/tmp 文件（崩溃残留）。

        受保护集合 = files 表登记文件 ∪ 已准备（未发布）作业的私有输出 ~job。
        准备写盘后事务前强退的输出不在保护集内，按孤儿清理（旧版本完整无损）。
        """
        conn = self._conn()
        protected = {f"{row['id']}.parquet" for row in conn.execute("SELECT id FROM files")}
        protected.update(
            f"~{row['job']}.parquet"
            for row in conn.execute("SELECT job FROM jobs WHERE state='prepared'")
        )
        for entry in self.parquet_dir.iterdir():
            if ".tmp." in entry.name or entry.name not in protected:
                entry.unlink()

    # ------------------------------------------------------------------ 工具
    def _current_version(self, conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT current_version FROM meta WHERE id=1").fetchone()[0])

    def _bump_version(self, conn: sqlite3.Connection) -> int:
        conn.execute("UPDATE meta SET current_version = current_version + 1 WHERE id=1")
        return self._current_version(conn)

    @staticmethod
    def _rollback(conn: sqlite3.Connection) -> None:
        """异常路径上的兜底回滚：回滚自身失败不应掩盖原始异常。"""
        try:
            self._rollback(conn)
        except sqlite3.Error:
            pass

    def _file_path(self, file_id: str) -> Path:
        return self.parquet_dir / f"{file_id}.parquet"

    def _atomic_write_parquet(self, file_id: str, values: list[Optional[int]]) -> Path:
        """先写 .tmp 并 fsync，再原子 rename，保证落盘完整或不存在。"""
        table = pa.table({"value": pa.array(values, type=pa.int64())}, schema=SCHEMA)
        tmp = self.parquet_dir / f"{file_id}.tmp.{os.getpid()}.{threading.get_ident()}"
        pq.write_table(table, tmp, compression="snappy")
        os.sync()
        final = self._file_path(file_id)
        os.replace(tmp, final)
        os.sync()
        return final

    # ------------------------------------------------------------------ 追加
    def append(self, obj: dict[str, Any]) -> dict[str, Any]:
        _require_members(obj, {"file", "values"})
        if "file" not in obj:
            raise ApiError(400, "缺少成员 file", "$.file")
        if "values" not in obj:
            raise ApiError(400, "缺少成员 values", "$.values")

        file_id = obj["file"]
        if not isinstance(file_id, str) or not _is_ascii_alnum(file_id):
            raise ApiError(400, "file 必须是非空 ASCII 字母数字串", "$.file")

        values_raw = obj["values"]
        if not isinstance(values_raw, list):
            raise ApiError(400, "values 必须是数组", "$.values")
        values: list[Optional[int]] = []
        for i, v in enumerate(values_raw):
            # bool 是 int 的子类，必须显式拒绝
            if isinstance(v, bool) or not isinstance(v, int):
                if v is not None:
                    raise ApiError(400, "values 元素只能是 int64 或 null",
                                   _index_path("$.values", i))
                values.append(None)
            elif not (INT64_MIN <= v <= INT64_MAX):
                raise ApiError(400, "整数超出 int64 范围", _index_path("$.values", i))
            else:
                values.append(v)

        with self._write_lock:
            conn = self._conn()
            # 标识永不复用：即使文件已被替换也拒绝
            exists = conn.execute("SELECT 1 FROM files WHERE id=?", (file_id,)).fetchone()
            if exists:
                raise ApiError(409, f"文件标识 {file_id!r} 已被使用，永不复用", "$.file")

            self._atomic_write_parquet(file_id, values)
            conn.execute("BEGIN IMMEDIATE")
            try:
                version = self._bump_version(conn)
                conn.execute(
                    "INSERT INTO files (id, nrows, created_ver) VALUES (?, ?, ?)",
                    (file_id, len(values), version),
                )
                conn.execute("COMMIT")
            except BaseException:
                self._rollback(conn)
                raise
        return {"version": version}

    # ------------------------------------------------------------------ 删除
    def delete(self, obj: dict[str, Any]) -> dict[str, Any]:
        _require_members(obj, {"positions"})
        if "positions" not in obj:
            raise ApiError(400, "缺少成员 positions", "$.positions")
        positions = obj["positions"]
        if not isinstance(positions, list):
            raise ApiError(400, "positions 必须是数组", "$.positions")

        parsed: list[tuple[str, int]] = []
        for i, item in enumerate(positions):
            p = _index_path("$.positions", i)
            if not isinstance(item, list) or len(item) != 2:
                raise ApiError(400, "位置项必须是形如 [file, pos] 的二元数组", p)
            file_id, pos = item[0], item[1]
            if not isinstance(file_id, str):
                raise ApiError(400, "位置项的 file 必须是字符串", _index_path(p, 0))
            # 布尔不是合法位置
            if isinstance(pos, bool) or not isinstance(pos, int):
                raise ApiError(400, "位置项的 pos 必须是整数", _index_path(p, 1))
            if pos < 0:
                raise ApiError(400, "pos 必须是零基非负整数", _index_path(p, 1))
            parsed.append((file_id, pos))

        with self._write_lock:
            conn = self._conn()
            # 先在锁内做全部引用校验：当前文件界内。拒绝则不改任何状态。
            for i, (file_id, pos) in enumerate(parsed):
                row = conn.execute(
                    "SELECT nrows FROM files WHERE id=? AND replaced_at IS NULL",
                    (file_id,),
                ).fetchone()
                if row is None:
                    raise ApiError(400, f"引用了不存在或已替换的文件 {file_id!r}",
                                   _index_path("$.positions", i))
                if pos >= row["nrows"]:
                    raise ApiError(
                        400,
                        f"pos={pos} 越界，文件 {file_id!r} 仅有 {row['nrows']} 行",
                        _index_path("$.positions", i),
                    )

            conn.execute("BEGIN IMMEDIATE")
            try:
                version = self._bump_version(conn)
                # 重复坐标 / 已删除坐标允许：INSERT OR IGNORE 天然幂等
                conn.executemany(
                    "INSERT OR IGNORE INTO tombstones (file, pos, del_ver) VALUES (?, ?, ?)",
                    [(f, p, version) for f, p in parsed],
                )
                conn.execute("COMMIT")
            except BaseException:
                self._rollback(conn)
                raise
        return {"version": version}

    # ------------------------------------------------------------------ 准备
    def prepare(self, obj: dict[str, Any]) -> dict[str, Any]:
        _require_members(obj, {"job", "files"})
        if "job" not in obj:
            raise ApiError(400, "缺少成员 job", "$.job")
        if "files" not in obj:
            raise ApiError(400, "缺少成员 files", "$.files")
        job = obj["job"]
        if not isinstance(job, str) or not _is_ascii_alnum(job):
            raise ApiError(400, "job 必须是非空 ASCII 字母数字串", "$.job")
        files = obj["files"]
        if not isinstance(files, list) or not files:
            raise ApiError(400, "files 必须是非空数组", "$.files")

        for i, f in enumerate(files):
            if not isinstance(f, str):
                raise ApiError(400, "files 元素必须是字符串（文件号）",
                               _index_path("$.files", i))

        with self._write_lock:
            conn = self._conn()
            if conn.execute("SELECT 1 FROM jobs WHERE job=?", (job,)).fetchone():
                # 标识永不复用（含已发布的 job）
                raise ApiError(409, f"作业标识 {job!r} 已被使用，永不复用", "$.job")
            if len(set(files)) != len(files):
                # 重复成员定位到第一个重复下标
                seen: set[str] = set()
                for i, f in enumerate(files):
                    if f in seen:
                        raise ApiError(400, "files 含重复文件号",
                                       _index_path("$.files", i))
                    seen.add(f)
            for i, f in enumerate(files):
                row = conn.execute(
                    "SELECT 1 FROM files WHERE id=? AND replaced_at IS NULL", (f,)
                ).fetchone()
                if row is None:
                    raise ApiError(400, f"引用了不存在或已替换的文件 {f!r}",
                                   _index_path("$.files", i))

            base = self._current_version(conn)
            ordered = sorted(files)  # ASCII 序（Python str 排序即 Unicode 码点序）

            # 关键：位置映射覆盖输入文件的【全部行】（含已删除行），
            # 旧 pos 与新 pos 一一对应、不压缩。私有 Parquet 按合并坐标逐槽写入：
            # 存活行写原值，已删除行写 null 占位，保证新坐标 == 合并序列下标。
            # 这样准备之后才删除的旧坐标，发布时映射到同一新坐标墓碑化，
            # 幸存行坐标保持不变（见规格示例：删 [a,0] 后 7 仍在 ~j 的 pos 1）。
            merged: list[Optional[int]] = []
            mapping: list[tuple[str, str, int, int]] = []
            new_pos = 0
            for f in ordered:
                nrows = int(conn.execute(
                    "SELECT nrows FROM files WHERE id=?", (f,)).fetchone()[0])
                dead = {
                    r["pos"]
                    for r in conn.execute(
                        "SELECT pos FROM tombstones WHERE file=?", (f,))
                }
                col = pq.read_table(self._file_path(f), columns=["value"]).column("value")
                for old_pos in range(nrows):
                    mapping.append((job, f, old_pos, new_pos))
                    if old_pos in dead:
                        merged.append(None)  # 占位槽，发布时同样落墓碑
                    else:
                        merged.append(col[old_pos].as_py())
                    new_pos += 1

            output_id = "~" + job  # 私有文件名，'~' 不可能与用户 file/job 冲突
            self._atomic_write_parquet(output_id, merged)
            self._crash("prepare_write")

            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO jobs (job, state, base_version, created_ver) "
                    "VALUES (?, 'prepared', ?, ?)",
                    (job, base, base),
                )
                conn.executemany(
                    "INSERT INTO job_inputs (job, file, file_seq) VALUES (?, ?, ?)",
                    [(job, f, seq) for seq, f in enumerate(ordered)],
                )
                conn.executemany(
                    "INSERT INTO job_mapping (job, old_file, old_pos, new_pos) "
                    "VALUES (?, ?, ?, ?)",
                    mapping,
                )
                conn.execute("COMMIT")
            except BaseException:
                self._rollback(conn)
                raise
        return {"base": base}

    # ------------------------------------------------------------------ 发布
    def publish(self, obj: dict[str, Any]) -> dict[str, Any]:
        _require_members(obj, {"job"})
        if "job" not in obj:
            raise ApiError(400, "缺少成员 job", "$.job")
        job = obj["job"]
        if not isinstance(job, str):
            raise ApiError(400, "job 必须是字符串", "$.job")

        with self._write_lock:
            conn = self._conn()
            row = conn.execute("SELECT * FROM jobs WHERE job=?", (job,)).fetchone()
            if row is None:
                raise ApiError(400, f"未知作业 {job!r}", "$.job")

            # 同 job 发布重试：幂等返回原版本，不增版本
            if row["state"] == "published":
                return {"version": int(row["published_ver"])}

            inputs = [
                r["file"]
                for r in conn.execute(
                    "SELECT file FROM job_inputs WHERE job=? ORDER BY file_seq", (job,))
            ]
            # 任一输入文件已被（其他作业）替换 -> 409；本作业尚未提交故不会替换自己
            replaced = conn.execute(
                "SELECT file FROM job_inputs WHERE job=? AND file IN "
                "(SELECT id FROM files WHERE replaced_at IS NOT NULL)",
                (job,),
            ).fetchall()
            if replaced:
                names = ", ".join(r["file"] for r in replaced)
                raise ApiError(409, f"输入文件已被替换: {names}", "$.job")

            output_id = "~" + job
            # 输出文件为全部输入行的合并槽位（死行为 null 占位），行数即映射行数。
            nrows = int(conn.execute(
                "SELECT COUNT(*) FROM job_mapping WHERE job=?", (job,)).fetchone()[0])

            self._crash("publish_before_commit")

            conn.execute("BEGIN IMMEDIATE")
            try:
                version = self._bump_version(conn)
                # 仅替换这些输入文件，其他文件不动
                conn.executemany(
                    "UPDATE files SET replaced_at=?, replaced_job=? WHERE id=?",
                    [(version, job, f) for f in inputs],
                )
                conn.execute(
                    "INSERT INTO files (id, nrows, created_ver) VALUES (?, ?, ?)",
                    (output_id, nrows, version),
                )
                # 准备之后对旧坐标的删除映射至新位置：
                # 映射中存在的旧坐标若已被删，则其新坐标在输出文件中同样墓碑化。
                conn.execute(
                    "INSERT OR IGNORE INTO tombstones (file, pos, del_ver) "
                    "SELECT ?, m.new_pos, ? "
                    "FROM job_mapping m "
                    "WHERE m.job=? AND EXISTS ("
                    "  SELECT 1 FROM tombstones t "
                    "  WHERE t.file=m.old_file AND t.pos=m.old_pos)",
                    (output_id, version, job),
                )
                conn.execute(
                    "UPDATE jobs SET state='published', published_ver=? WHERE job=?",
                    (version, job),
                )
                conn.execute("COMMIT")
            except BaseException:
                self._rollback(conn)
                raise

            self._crash("publish_after_commit")
        return {"version": version}
    # ------------------------------------------------------------------ 读取
    def read(self, obj: dict[str, Any]) -> dict[str, Any]:
        _require_members(obj, {"version"})
        if "version" not in obj:
            raise ApiError(400, "缺少成员 version", "$.version")
        version = obj["version"]
        if isinstance(version, bool) or not isinstance(version, int):
            raise ApiError(400, "version 必须是整数", "$.version")

        conn = self._conn()
        current = self._current_version(conn)
        if version < 0 or version > current:
            raise ApiError(400,
                           f"version 越界：当前为 {current}，请求 {version}",
                           "$.version")

        # 单条只读事务保证快照一致（SQLite WAL 下不阻塞写者）
        conn.execute("BEGIN")
        try:
            files = [
                r["id"]
                for r in conn.execute(
                    "SELECT id FROM files "
                    "WHERE created_ver<=? AND (replaced_at IS NULL OR replaced_at>?) "
                    "ORDER BY id",
                    (version, version),
                )
            ]
            rows: list[dict[str, Any]] = []
            for f in files:
                dead = {
                    r["pos"]
                    for r in conn.execute(
                        "SELECT pos FROM tombstones WHERE file=? AND del_ver<=?",
                        (f, version),
                    )
                }
                nrows = int(conn.execute(
                    "SELECT nrows FROM files WHERE id=?", (f,)).fetchone()[0])
                col = pq.read_table(self._file_path(f), columns=["value"]).column("value")
                for pos in range(nrows):
                    if pos in dead:
                        continue
                    rows.append({"file": f, "pos": pos, "value": col[pos].as_py()})
            conn.execute("COMMIT")
        except BaseException:
            self._rollback(conn)
            raise
        # files 已按 id ASCII 序，行内按 pos 升序 -> 整体满足排序要求
        return {"version": version, "rows": rows}


# ---------------------------------------------------------------------- HTTP 层
class Handler(BaseHTTPRequestHandler):
    server_version = "LakeComp/1.0"
    store: Store  # 由 LakeHTTPServer 注入

    def log_message(self, fmt: str, *args: Any) -> None:  # 日志走 stderr
        import sys
        print(f"[{self.log_date_time_string()}] {fmt % args}", file=sys.stderr)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        # 精确路径匹配，其余 404
        route = {
            "/append": self.store.append,
            "/delete": self.store.delete,
            "/prepare": self.store.prepare,
            "/publish": self.store.publish,
            "/read": self.store.read,
        }.get(self.path)
        if route is None:
            self._send_json(404, {"error": f"未知路径 {self.path!r}", "path": "$"})
            return
        try:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except (TypeError, ValueError):
                raise ApiError(400, "Content-Length 非法", "$")
            if length <= 0:
                raise ApiError(400, "缺少请求体", "$")
            if length > MAX_BODY_BYTES:
                raise ApiError(400, "请求体超过大小上限", "$")
            raw = self.rfile.read(length)
            obj = parse_request_body(raw)
            result = route(obj)
            self._send_json(200, result)
        except ApiError as exc:
            self._send_json(exc.status, {"error": exc.message, "path": exc.path})
        except Exception as exc:  # 兜底：绝不泄露栈，状态保持不变
            import sys
            import traceback
            traceback.print_exc(file=sys.stderr)
            self._send_json(500, {"error": f"内部错误: {exc}", "path": "$"})

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        self._send_json(404, {"error": "仅支持 POST 接口与 GET /health", "path": "$"})


class LakeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr: tuple[str, int], store: Store):
        super().__init__(addr, Handler)
        self.store = store
        Handler.store = store


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="湖表压实 HTTP 后端")
    parser.add_argument("--host", default=os.environ.get("LAKECOMP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("LAKECOMP_PORT", "8080")))
    parser.add_argument("--data", default=os.environ.get("LAKECOMP_DATA", "./data"))
    args = parser.parse_args(argv)

    store = Store(args.data)
    httpd = LakeHTTPServer((args.host, args.port), store)
    print(f"湖表压实后端监听 http://{args.host}:{args.port} 数据目录={store.data_dir}")

    def _shutdown(signum: int, frame: Any) -> None:
        # 由主线程在信号中关闭 server，进行中的请求线程随 daemon 退出
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
