"""Backend logic for the math practice app."""

import random
from dataclasses import dataclass, field
from pathlib import Path

from mathkg.config import PracticeConfig
from mathkg.store import ProblemStore


@dataclass
class LoadResult:
    total_loaded: int
    missing_ids: list[str] = field(default_factory=list)


def check_answer(user_answer: str, correct_solution: str) -> bool:
    """Check if user_answer matches correct_solution (case-insensitive substring)."""
    return user_answer.strip().lower() in correct_solution.strip().lower()


class PracticeSession:
    """Manages problem loading, selection, and eval logging for a practice session."""

    def __init__(self, store: ProblemStore, config: PracticeConfig):
        self._store = store
        self._config = config

        batch = store.get_batch_by_name(config.batch_name)
        if batch is None:
            raise ValueError(f"Batch '{config.batch_name}' not found in database.")
        self._batch_name: str = batch["batch_name"]
        self._batch_id: str = batch["id"]

        self._problems: dict[str, list[dict]] = {}
        self._available_grades: list[str] = []

    @property
    def batch_name(self) -> str:
        return self._batch_name

    @property
    def batch_id(self) -> str:
        return self._batch_id

    @property
    def available_grades(self) -> list[str]:
        return self._available_grades

    @property
    def is_problem_list_mode(self) -> bool:
        return self._config.problem_list is not None

    def load_problems(self) -> LoadResult:
        """Load all problems into the session. Uses problem-list or batch mode."""
        if self._config.problem_list is not None:
            return self._load_from_problem_list(self._config.problem_list)
        return self._load_all_grades()

    def _load_all_grades(self) -> LoadResult:
        """Load problems for all grades in the batch."""
        grades = self._store.get_available_grades(self._batch_id)
        self._available_grades = grades
        self._problems = {}
        total = 0
        for grade in grades:
            problems = self._load_grade_problems(grade)
            self._problems[grade] = problems
            total += len(problems)
        return LoadResult(total_loaded=total)

    def _load_from_problem_list(self, filename: str) -> LoadResult:
        """Load a curated set of problems by ID from a text file."""
        filepath = Path("data") / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Problem list file not found: {filepath}")

        ids = _read_problem_ids(filepath)
        if not ids:
            raise ValueError(f"Problem list file is empty: {filepath}")

        problems = self._store.get_problems_by_ids(ids)
        if not problems:
            raise ValueError(
                "No matching problems found in the database for the given IDs."
            )

        found_ids = {p["id"] for p in problems}
        missing = [pid for pid in ids if pid not in found_ids]

        _normalize_problems(problems)

        by_grade: dict[str, list[dict]] = {}
        for p in problems:
            by_grade.setdefault(p["grade_level"], []).append(p)
        for grade_problems in by_grade.values():
            random.shuffle(grade_problems)

        self._problems = by_grade
        self._available_grades = sorted(by_grade.keys())

        return LoadResult(total_loaded=len(problems), missing_ids=missing)

    def load_problems_for_grade(self, grade_level: str) -> list[dict]:
        """Load problems from DB for a grade (used for reloading an exhausted grade)."""
        problems = self._load_grade_problems(grade_level)
        self._problems[grade_level] = problems
        return problems

    def _load_grade_problems(self, grade_level: str) -> list[dict]:
        """Fetch, normalize, and shuffle problems for a single grade."""
        problems = self._store.get_problems_for_grade(
            self._batch_id, grade_level, limit_per_std=self._config.probs_per_std
        )
        _normalize_problems(problems)
        random.shuffle(problems)
        return problems

    def get_remaining_count(self, grade: str) -> int:
        return len(self._problems.get(grade, []))

    def get_next_problem(self, grade: str) -> dict | None:
        """Pop and return the next problem for a grade, or None if exhausted.

        In batch mode, auto-reloads from DB when the grade pool is empty.
        In problem-list mode, returns None when exhausted (no reload).
        """
        pool = self._problems.get(grade, [])
        if pool:
            return pool.pop()

        if self.is_problem_list_mode:
            return None

        # Batch mode: reload from DB
        reloaded = self.load_problems_for_grade(grade)
        if reloaded:
            return reloaded.pop()
        return None

def _normalize_problems(problems: list[dict]) -> None:
    """Add UI-friendly field aliases (problem, solution, standard) in-place."""
    for p in problems:
        p["problem"] = p.get("problem_text", "")
        p["solution"] = p.get("solution_text", "")
        p["standard"] = p.get("standard_code", "")


def _read_problem_ids(filepath: Path) -> list[str]:
    """Read problem IDs from a text file, skipping blanks and comments."""
    ids = []
    for line in filepath.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            ids.append(line)
    return ids
