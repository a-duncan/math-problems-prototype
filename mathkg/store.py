"""SQLite persistence layer for problems and evaluation results."""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS batches (
    id              TEXT PRIMARY KEY,
    batch_name      TEXT NOT NULL,
    model_name      TEXT,
    temperature     REAL DEFAULT 0.0,
    num_problems    INTEGER,
    grades_requested TEXT,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS problems (
    id              TEXT PRIMARY KEY,
    batch_id        TEXT NOT NULL REFERENCES batches(id),
    grade_level     TEXT NOT NULL,
    standard_code   TEXT NOT NULL,
    problem_text    TEXT NOT NULL,
    choices         TEXT NOT NULL,
    solution_text   TEXT NOT NULL,
    position        INTEGER,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_problems_batch ON problems(batch_id);
CREATE INDEX IF NOT EXISTS idx_problems_grade_std ON problems(grade_level, standard_code);

CREATE TABLE IF NOT EXISTS eval_runs (
    id              TEXT PRIMARY KEY,
    eval_type       TEXT NOT NULL,
    source_batch_id TEXT REFERENCES batches(id),
    model_name      TEXT,
    eval_run_label  TEXT,
    eval_batch_size INTEGER,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS eval_results (
    id              TEXT PRIMARY KEY,
    eval_run_id     TEXT NOT NULL REFERENCES eval_runs(id),
    problem_id      TEXT REFERENCES problems(id),
    grade_level     TEXT,
    standard_code   TEXT,
    problem_text    TEXT,
    choices         TEXT,
    solution_text   TEXT,
    bad_answer_matching INTEGER,
    incorrect_answer    INTEGER,
    multiple_correct    INTEGER,
    ambiguous           INTEGER,
    missing_info        INTEGER,
    answer_in_q         INTEGER,
    open_ended          INTEGER,
    needs_graphic       INTEGER,
    bad_format      INTEGER,
    unanswerable    INTEGER,
    other_issue     INTEGER,
    bad_problem     INTEGER,
    problem_correct INTEGER,
    user_answer     TEXT,
    comments        TEXT,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eval_results_run ON eval_results(eval_run_id);
CREATE INDEX IF NOT EXISTS idx_eval_results_problem ON eval_results(problem_id);
"""


def _new_id() -> str:
    return uuid.uuid4().hex


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


class ProblemStore:
    """SQLite-backed store for problems and evaluation results."""

    def __init__(self, db_path: str | Path = "data/mathkg.db"):
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA_SQL)
        # Idempotent column migrations for existing databases
        try:
            self._conn.execute("ALTER TABLE eval_runs ADD COLUMN eval_run_label TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        self._conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._conn.close()

    def commit(self):
        self._conn.commit()

    # -- Batches --

    def create_batch(
        self,
        batch_name: str,
        model_name: str | None = None,
        temperature: float = 0.0,
        num_problems: int | None = None,
        grades_requested: list[str] | None = None,
    ) -> str:
        batch_id = _new_id()
        self._conn.execute(
            """INSERT INTO batches (id, batch_name, model_name, temperature,
               num_problems, grades_requested, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                batch_id,
                batch_name,
                model_name,
                temperature,
                num_problems,
                json.dumps(grades_requested) if grades_requested else None,
                _now_iso(),
            ),
        )
        self._conn.commit()
        return batch_id

    def get_batch_by_name(self, name: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM batches WHERE batch_name = ?", (name,)
        ).fetchone()
        return _row_to_dict(row) if row else None

    def list_batches(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM batches ORDER BY created_at"
        ).fetchall()
        return [_row_to_dict(r) for r in rows]

    # -- Problems --

    def insert_problems(
        self,
        batch_id: str,
        grade_level: str,
        standard_code: str,
        problems: list[dict],
    ) -> list[str]:
        ids = []
        now = _now_iso()
        for i, p in enumerate(problems):
            pid = _new_id()
            self._conn.execute(
                """INSERT INTO problems (id, batch_id, grade_level, standard_code,
                   problem_text, choices, solution_text, position, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    pid,
                    batch_id,
                    grade_level,
                    standard_code,
                    p.get("problem", p.get("problem_text", "")),
                    json.dumps(p.get("choices", [])),
                    p.get("solution", p.get("solution_text", "")),
                    i,
                    now,
                ),
            )
            ids.append(pid)
        return ids

    def get_problems_by_batch(self, batch_id: str) -> dict[str, dict[str, list[dict]]]:
        rows = self._conn.execute(
            """SELECT * FROM problems WHERE batch_id = ?
               ORDER BY grade_level, standard_code, position""",
            (batch_id,),
        ).fetchall()
        result: dict[str, dict[str, list[dict]]] = {}
        for row in rows:
            d = _row_to_dict(row)
            d["choices"] = json.loads(d["choices"])
            grade = d["grade_level"]
            std = d["standard_code"]
            result.setdefault(grade, {}).setdefault(std, []).append(d)
        return result

    def get_problems_for_grade(
        self, batch_id: str, grade_level: str, limit_per_std: int | None = None
    ) -> list[dict]:
        rows = self._conn.execute(
            """SELECT * FROM problems
               WHERE batch_id = ? AND grade_level = ?
               ORDER BY standard_code, position""",
            (batch_id, grade_level),
        ).fetchall()

        if limit_per_std is None:
            result = []
            for row in rows:
                d = _row_to_dict(row)
                d["choices"] = json.loads(d["choices"])
                result.append(d)
            return result

        # Group by standard and limit
        by_std: dict[str, list[dict]] = {}
        for row in rows:
            d = _row_to_dict(row)
            d["choices"] = json.loads(d["choices"])
            by_std.setdefault(d["standard_code"], []).append(d)

        result = []
        for problems in by_std.values():
            result.extend(problems[:limit_per_std])
        return result

    def get_available_grades(self, batch_id: str) -> list[str]:
        rows = self._conn.execute(
            """SELECT DISTINCT grade_level FROM problems
               WHERE batch_id = ? ORDER BY grade_level""",
            (batch_id,),
        ).fetchall()
        return [row["grade_level"] for row in rows]

    def get_problems_by_ids(self, problem_ids: list[str]) -> list[dict]:
        """Fetch problems by their UUIDs (across any batch)."""
        if not problem_ids:
            return []
        placeholders = ",".join("?" for _ in problem_ids)
        rows = self._conn.execute(
            f"SELECT * FROM problems WHERE id IN ({placeholders})",
            problem_ids,
        ).fetchall()
        result = []
        for row in rows:
            d = _row_to_dict(row)
            d["choices"] = json.loads(d["choices"])
            result.append(d)
        return result

    # -- Eval Runs --

    def create_eval_run(
        self,
        source_batch_id: str,
        eval_type: str = "auto",
        model_name: str | None = None,
        eval_run_label: str | None = None,
        eval_batch_size: int | None = None,
    ) -> str:
        run_id = _new_id()
        self._conn.execute(
            """INSERT INTO eval_runs (id, eval_type, source_batch_id,
               model_name, eval_run_label, eval_batch_size, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (run_id, eval_type, source_batch_id, model_name, eval_run_label, eval_batch_size, _now_iso()),
        )
        self._conn.commit()
        return run_id

    # -- Eval Results --

    def insert_eval_results(self, eval_run_id: str, results: list[dict]) -> list[str]:
        ids = []
        now = _now_iso()
        for r in results:
            rid = _new_id()
            self._conn.execute(
                """INSERT INTO eval_results (id, eval_run_id, problem_id,
                   grade_level, standard_code, problem_text, choices, solution_text,
                   bad_answer_matching, incorrect_answer, multiple_correct,
                   ambiguous, missing_info, answer_in_q, open_ended, needs_graphic,
                   bad_format, unanswerable, other_issue, bad_problem,
                   problem_correct, user_answer, comments, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rid,
                    eval_run_id,
                    r.get("problem_id"),
                    r.get("grade_level"),
                    r.get("standard_code"),
                    r.get("problem_text"),
                    json.dumps(r["choices"]) if isinstance(r.get("choices"), list) else r.get("choices"),
                    r.get("solution_text"),
                    int(r["bad_answer_matching"]) if r.get("bad_answer_matching") is not None else None,
                    int(r["incorrect_answer"]) if r.get("incorrect_answer") is not None else None,
                    int(r["multiple_correct"]) if r.get("multiple_correct") is not None else None,
                    int(r["ambiguous"]) if r.get("ambiguous") is not None else None,
                    int(r["missing_info"]) if r.get("missing_info") is not None else None,
                    int(r["answer_in_q"]) if r.get("answer_in_q") is not None else None,
                    int(r["open_ended"]) if r.get("open_ended") is not None else None,
                    int(r["needs_graphic"]) if r.get("needs_graphic") is not None else None,
                    int(r["bad_format"]) if r.get("bad_format") is not None else None,
                    int(r["unanswerable"]) if r.get("unanswerable") is not None else None,
                    int(r["other_issue"]) if r.get("other_issue") is not None else None,
                    int(r["bad_problem"]) if r.get("bad_problem") is not None else None,
                    int(r["problem_correct"]) if r.get("problem_correct") is not None else None,
                    r.get("user_answer"),
                    r.get("comments"),
                    now,
                ),
            )
            ids.append(rid)
        return ids

    def get_eval_results(
        self,
        eval_run_id: str | None = None,
        batch_id: str | None = None,
        grade_level: str | None = None,
        standard_code: str | None = None,
    ) -> list[dict]:
        query = (
            "SELECT er.*, b.batch_name, ru.eval_run_label"
            " FROM eval_results er"
            " JOIN eval_runs ru ON er.eval_run_id = ru.id"
            " JOIN batches b ON ru.source_batch_id = b.id"
        )
        params: list = []
        conditions = []

        if batch_id is not None:
            conditions.append("ru.source_batch_id = ?")
            params.append(batch_id)

        if eval_run_id is not None:
            conditions.append("er.eval_run_id = ?")
            params.append(eval_run_id)
        if grade_level is not None:
            conditions.append("er.grade_level = ?")
            params.append(grade_level)
        if standard_code is not None:
            conditions.append("er.standard_code = ?")
            params.append(standard_code)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY er.created_at"

        rows = self._conn.execute(query, params).fetchall()
        results = []
        for row in rows:
            d = _row_to_dict(row)
            if d.get("choices"):
                try:
                    d["choices"] = json.loads(d["choices"])
                except (json.JSONDecodeError, TypeError):
                    pass
            # Convert integer bools back to Python bools
            for col in (
                "bad_answer_matching", "incorrect_answer", "multiple_correct",
                "ambiguous", "missing_info", "answer_in_q", "open_ended",
                "needs_graphic", "bad_format", "unanswerable", "other_issue",
                "bad_problem", "problem_correct",
            ):
                if d.get(col) is not None:
                    d[col] = bool(d[col])
            results.append(d)
        return results
