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

CREATE TABLE IF NOT EXISTS eval_progress (
    id                    TEXT PRIMARY KEY,
    eval_run_id           TEXT NOT NULL REFERENCES eval_runs(id),
    problem_id            TEXT NOT NULL REFERENCES problems(id),
    source_eval_result_id TEXT REFERENCES eval_results(id),
    grade_level           TEXT NOT NULL,
    standard_code         TEXT NOT NULL,
    position              INTEGER NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending',
    human_eval_result_id  TEXT REFERENCES eval_results(id),
    completed_at          TEXT,
    created_at            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eval_progress_run
    ON eval_progress(eval_run_id);
CREATE INDEX IF NOT EXISTS idx_eval_progress_run_status
    ON eval_progress(eval_run_id, status);
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

    # -- Eval Run queries --

    def get_eval_run(self, eval_run_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM eval_runs WHERE id = ?", (eval_run_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None

    def get_eval_run_by_label(self, label: str) -> dict | None:
        """Fetch most recent eval run with the given label."""
        row = self._conn.execute(
            "SELECT * FROM eval_runs WHERE eval_run_label = ? ORDER BY created_at DESC LIMIT 1",
            (label,),
        ).fetchone()
        return _row_to_dict(row) if row else None

    def list_eval_runs(self, eval_type: str | None = None) -> list[dict]:
        if eval_type is not None:
            rows = self._conn.execute(
                "SELECT * FROM eval_runs WHERE eval_type = ? ORDER BY created_at",
                (eval_type,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM eval_runs ORDER BY created_at"
            ).fetchall()
        return [_row_to_dict(r) for r in rows]

    # -- Eval Progress CRUD --

    def seed_eval_progress(
        self,
        eval_run_id: str,
        source_eval_run_id: str | None,
        batch_id: str,
        probs_per_std: int | None = None,
        grades: list[str] | None = None,
    ) -> int:
        """Seed progress rows from auto-eval results (or problems if no source_eval_run_id).
        Returns count seeded."""
        now = _now_iso()

        if source_eval_run_id is not None:
            # Source from eval_results
            query = """
                SELECT er.id as source_id, er.problem_id, er.grade_level, er.standard_code
                FROM eval_results er
                WHERE er.eval_run_id = ?
                ORDER BY er.grade_level, er.standard_code, er.created_at
            """
            params: list = [source_eval_run_id]
            if grades:
                placeholders = ",".join("?" for _ in grades)
                query = query.replace(
                    "WHERE er.eval_run_id = ?",
                    f"WHERE er.eval_run_id = ? AND er.grade_level IN ({placeholders})"
                )
                params = [source_eval_run_id] + list(grades)
            rows = self._conn.execute(query, params).fetchall()
            items = [_row_to_dict(r) for r in rows]
        else:
            # Source from problems table
            query = """
                SELECT p.id as problem_id, p.grade_level, p.standard_code, NULL as source_id
                FROM problems p
                WHERE p.batch_id = ?
                ORDER BY p.grade_level, p.standard_code, p.position
            """
            params = [batch_id]
            if grades:
                placeholders = ",".join("?" for _ in grades)
                query = query.replace(
                    "WHERE p.batch_id = ?",
                    f"WHERE p.batch_id = ? AND p.grade_level IN ({placeholders})"
                )
                params = [batch_id] + list(grades)
            rows = self._conn.execute(query, params).fetchall()
            items = [_row_to_dict(r) for r in rows]

        # Apply probs_per_std filter
        if probs_per_std is not None:
            by_std: dict[str, list] = {}
            for item in items:
                key = (item["grade_level"], item["standard_code"])
                by_std.setdefault(key, []).append(item)
            filtered: list[dict] = []
            for std_items in by_std.values():
                filtered.extend(std_items[:probs_per_std])
            items = filtered

        for position, item in enumerate(items):
            self._conn.execute(
                """INSERT INTO eval_progress
                   (id, eval_run_id, problem_id, source_eval_result_id,
                    grade_level, standard_code, position, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                (
                    _new_id(),
                    eval_run_id,
                    item["problem_id"],
                    item.get("source_id"),
                    item["grade_level"],
                    item["standard_code"],
                    position,
                    now,
                ),
            )
        return len(items)

    def get_eval_progress(
        self, eval_run_id: str, status: str | None = None
    ) -> list[dict]:
        """Get progress rows for a run, ordered by grade/standard/position."""
        if status is not None:
            rows = self._conn.execute(
                """SELECT * FROM eval_progress
                   WHERE eval_run_id = ? AND status = ?
                   ORDER BY grade_level, standard_code, position""",
                (eval_run_id, status),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """SELECT * FROM eval_progress
                   WHERE eval_run_id = ?
                   ORDER BY grade_level, standard_code, position""",
                (eval_run_id,),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]

    _EVAL_FLAG_COLS = (
        "bad_answer_matching", "incorrect_answer", "multiple_correct",
        "ambiguous", "missing_info", "answer_in_q", "open_ended",
        "needs_graphic", "bad_format", "unanswerable", "other_issue",
        "bad_problem", "problem_correct",
    )

    def get_eval_progress_item(self, progress_id: str) -> dict | None:
        """Enriched item: JOINs eval_progress + problems + eval_results(auto+human).
        Returns dict with progress fields + problem_* + auto_* + human_* prefixed keys."""
        row = self._conn.execute(
            """
            SELECT
                ep.id, ep.eval_run_id, ep.problem_id, ep.source_eval_result_id,
                ep.grade_level, ep.standard_code, ep.position, ep.status,
                ep.human_eval_result_id, ep.completed_at, ep.created_at,
                p.problem_text  AS problem_problem_text,
                p.choices       AS problem_choices,
                p.solution_text AS problem_solution_text,
                ae.id                   AS auto_id,
                ae.bad_answer_matching  AS auto_bad_answer_matching,
                ae.incorrect_answer     AS auto_incorrect_answer,
                ae.multiple_correct     AS auto_multiple_correct,
                ae.ambiguous            AS auto_ambiguous,
                ae.missing_info         AS auto_missing_info,
                ae.answer_in_q          AS auto_answer_in_q,
                ae.open_ended           AS auto_open_ended,
                ae.needs_graphic        AS auto_needs_graphic,
                ae.bad_format           AS auto_bad_format,
                ae.unanswerable         AS auto_unanswerable,
                ae.other_issue          AS auto_other_issue,
                ae.bad_problem          AS auto_bad_problem,
                ae.comments             AS auto_comments,
                he.id                   AS human_id,
                he.bad_answer_matching  AS human_bad_answer_matching,
                he.incorrect_answer     AS human_incorrect_answer,
                he.multiple_correct     AS human_multiple_correct,
                he.ambiguous            AS human_ambiguous,
                he.missing_info         AS human_missing_info,
                he.answer_in_q          AS human_answer_in_q,
                he.open_ended           AS human_open_ended,
                he.needs_graphic        AS human_needs_graphic,
                he.bad_format           AS human_bad_format,
                he.unanswerable         AS human_unanswerable,
                he.other_issue          AS human_other_issue,
                he.bad_problem          AS human_bad_problem,
                he.user_answer          AS human_user_answer,
                he.comments             AS human_comments,
                he.problem_correct      AS human_problem_correct
            FROM eval_progress ep
            LEFT JOIN problems p ON ep.problem_id = p.id
            LEFT JOIN eval_results ae ON ep.source_eval_result_id = ae.id
            LEFT JOIN eval_results he ON ep.human_eval_result_id = he.id
            WHERE ep.id = ?
            """,
            (progress_id,),
        ).fetchone()
        if row is None:
            return None
        d = _row_to_dict(row)
        if d.get("problem_choices"):
            try:
                d["problem_choices"] = json.loads(d["problem_choices"])
            except (json.JSONDecodeError, TypeError):
                pass
        for prefix in ("auto_", "human_"):
            for col in self._EVAL_FLAG_COLS:
                key = f"{prefix}{col}"
                if d.get(key) is not None:
                    d[key] = bool(d[key])
        return d

    def complete_eval_progress(
        self, progress_id: str, human_eval_result_id: str
    ) -> None:
        """Set status='completed', human_eval_result_id, completed_at."""
        self._conn.execute(
            """UPDATE eval_progress
               SET status = 'completed', human_eval_result_id = ?, completed_at = ?
               WHERE id = ?""",
            (human_eval_result_id, _now_iso(), progress_id),
        )

    def update_eval_result(self, eval_result_id: str, updates: dict) -> None:
        """Update specified columns on an existing eval_results row."""
        if not updates:
            return
        set_clause = ", ".join(f"{col} = ?" for col in updates)
        values = list(updates.values()) + [eval_result_id]
        self._conn.execute(
            f"UPDATE eval_results SET {set_clause} WHERE id = ?", values
        )
