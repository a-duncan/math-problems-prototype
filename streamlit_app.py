"""
Math Practice App - Streamlit application for practicing grade-level math problems.
"""

import argparse
import random
import sys

import streamlit as st


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--human-eval", action="store_true", default=False,
                        help="Enable human evaluation mode")
    parser.add_argument("--batch-name", type=str, default="batch003",
                        help="Batch name to load problems from")
    parser.add_argument("--probs-per-std", type=int, default=2,
                        help="Number of problems per standard to load")
    parser.add_argument("--problem-list", type=str, default="batch003-good.txt",
                        help="Filename under data/ with one problem ID per line")
    return parser.parse_args(sys.argv[1:])


_args = _parse_args()

# === Configuration ===
HUMAN_EVAL_MODE = _args.human_eval
BATCH_NAME = _args.batch_name
PROBS_PER_STD = _args.probs_per_std
PROBLEM_LIST = _args.problem_list
DB_PATH = "data/mathkg.db"


def _get_store():
    """Get or create a ProblemStore in session state."""
    if "store" not in st.session_state:
        from mathkg.store import ProblemStore
        st.session_state.store = ProblemStore(DB_PATH)
    return st.session_state.store


# --- Startup: resolve batch ID ---

def _resolve_batch() -> tuple[str, str]:
    """Look up batch by name and return (batch_name, batch_id). Error out if not found."""
    store = _get_store()
    batch = store.get_batch_by_name(BATCH_NAME)
    if batch is None:
        st.error(f"Batch '{BATCH_NAME}' not found in database.")
        st.stop()
    return batch["batch_name"], batch["id"]


# --- Data loading helpers ---

def load_problems_for_grade(batch_id: str, grade_level: str, probs_per_std=None) -> list[dict]:
    """Load problems from SQLite for a given batch and grade."""
    store = _get_store()
    problems = store.get_problems_for_grade(batch_id, grade_level, limit_per_std=probs_per_std)
    # Normalize field names for the UI
    for p in problems:
        p["problem"] = p.get("problem_text", "")
        p["solution"] = p.get("solution_text", "")
        p["standard"] = p.get("standard_code", "")
    random.shuffle(problems)
    return problems


def _load_from_problem_list(filename: str):
    """Load a curated set of problems by ID from a text file."""
    from pathlib import Path

    filepath = Path("data") / filename
    if not filepath.exists():
        st.error(f"Problem list file not found: {filepath}")
        st.stop()

    # Read IDs, skip blanks and comments
    ids = []
    for line in filepath.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            ids.append(line)

    if not ids:
        st.error(f"Problem list file is empty: {filepath}")
        st.stop()

    store = _get_store()
    problems = store.get_problems_by_ids(ids)

    if not problems:
        st.error("No matching problems found in the database for the given IDs.")
        st.stop()

    # Warn about missing IDs
    found_ids = {p["id"] for p in problems}
    missing = [pid for pid in ids if pid not in found_ids]
    if missing:
        st.warning(f"{len(missing)} problem ID(s) not found in database.")

    # Normalize field names for the UI
    for p in problems:
        p["problem"] = p.get("problem_text", "")
        p["solution"] = p.get("solution_text", "")
        p["standard"] = p.get("standard_code", "")

    # Group by grade_level, shuffle within grade
    by_grade: dict[str, list[dict]] = {}
    for p in problems:
        by_grade.setdefault(p["grade_level"], []).append(p)
    for grade_problems in by_grade.values():
        random.shuffle(grade_problems)

    st.session_state.problems = by_grade
    st.session_state.available_grades = sorted(by_grade.keys())


def _load_all_grades(batch_id: str):
    """Load problems for all grades into session state at startup."""
    store = _get_store()
    grades = store.get_available_grades(batch_id)
    st.session_state.available_grades = grades
    st.session_state.problems = {}
    for grade in grades:
        st.session_state.problems[grade] = load_problems_for_grade(
            batch_id, grade, probs_per_std=PROBS_PER_STD
        )


def write_log_entry(bad_problem: bool, bad_reasons: dict, comments: str):
    """Write a human evaluation log entry to SQLite."""
    store = _get_store()
    problem = st.session_state.current_problem

    # Create eval run on first log entry
    if st.session_state.get("eval_run_id") is None:
        st.session_state.eval_run_id = store.create_eval_run(
            source_batch_id=BATCH_ID, eval_type="human",
            eval_run_label=f"{BATCH_NAME}_human",
        )

    result = {
        "problem_id": problem.get("id"),
        "grade_level": st.session_state.grade_level,
        "standard_code": problem.get("standard", ""),
        "problem_text": problem.get("problem", ""),
        "choices": problem.get("choices", []),
        "solution_text": problem.get("solution", ""),
        "bad_problem": bad_problem,
        "problem_correct": st.session_state.is_correct,
        "user_answer": st.session_state.user_answer,
        "bad_answer_matching": bad_reasons.get("bad_answer_matching", False),
        "bad_format": bad_reasons.get("bad_format", False),
        "incorrect_answer": bad_reasons.get("incorrect_answer", False),
        "ambiguous": bad_reasons.get("ambiguous", False),
        "missing_info": bad_reasons.get("missing_info", False),
        "answer_in_q": bad_reasons.get("answer_in_q", False),
        "open_ended": bad_reasons.get("open_ended", False),
        "needs_graphic": bad_reasons.get("needs_graphic", False),
        "unanswerable": bad_reasons.get("unanswerable", False),
        "other_issue": bad_reasons.get("other", False),
        "comments": comments,
    }

    store.insert_eval_results(st.session_state.eval_run_id, [result])
    store.commit()


def init_session_state():
    """Initialize session state variables."""
    if "current_problem" not in st.session_state:
        st.session_state.current_problem = None
    if "correct_count" not in st.session_state:
        st.session_state.correct_count = 0
    if "total_count" not in st.session_state:
        st.session_state.total_count = 0
    if "answered" not in st.session_state:
        st.session_state.answered = False
    if "is_correct" not in st.session_state:
        st.session_state.is_correct = None
    if "grade_level" not in st.session_state:
        st.session_state.grade_level = None
    if "user_answer" not in st.session_state:
        st.session_state.user_answer = ""
    if "eval_run_id" not in st.session_state:
        st.session_state.eval_run_id = None


def select_random_problem():
    """Select the next problem from the current grade's problem list."""
    grade = st.session_state.grade_level
    problems_by_grade = st.session_state.problems

    if grade and problems_by_grade.get(grade):
        st.session_state.current_problem = problems_by_grade[grade].pop()
        st.session_state.answered = False
        st.session_state.is_correct = None
        st.session_state.user_answer = ""
    elif grade:
        if PROBLEM_LIST is not None:
            # Problem-list mode — don't reload, just clear current problem
            st.session_state.current_problem = None
        else:
            # Grade list exhausted — reload from DB
            problems_by_grade[grade] = load_problems_for_grade(
                BATCH_ID, grade, probs_per_std=PROBS_PER_STD
            )
            if problems_by_grade[grade]:
                st.session_state.current_problem = problems_by_grade[grade].pop()
                st.session_state.answered = False
                st.session_state.is_correct = None
                st.session_state.user_answer = ""


def check_answer(user_answer: str):
    """Check if the user's answer matches the solution."""
    if st.session_state.current_problem is None:
        return

    correct_answers = st.session_state.current_problem["solution"].strip().lower()
    user_answer_clean = user_answer.strip().lower()

    st.session_state.answered = True
    st.session_state.total_count += 1
    st.session_state.user_answer = user_answer

    if user_answer_clean in correct_answers:
        st.session_state.is_correct = True
        st.session_state.correct_count += 1
    else:
        st.session_state.is_correct = False


# --- Resolve batch at module level (after _get_store is defined) ---
BATCH_NAME, BATCH_ID = _resolve_batch()


def main():
    title = "Math Practice - Human Eval Mode" if HUMAN_EVAL_MODE else "Math Practice"
    st.set_page_config(page_title="Math Practice", page_icon="🔢", layout="centered")

    st.title(title)

    init_session_state()

    # Preload problems on first run
    if "problems" not in st.session_state:
        if PROBLEM_LIST is not None:
            _load_from_problem_list(PROBLEM_LIST)
        else:
            _load_all_grades(BATCH_ID)

    available_grades = st.session_state.available_grades

    if not available_grades:
        st.error("No problems found for this batch. Please generate problems first.")
        return

    # Set default grade if not yet selected
    if st.session_state.grade_level is None:
        st.session_state.grade_level = available_grades[0]

    # Grade selection in sidebar
    with st.sidebar:
        st.header("Settings")

        current_index = (
            available_grades.index(st.session_state.grade_level)
            if st.session_state.grade_level in available_grades
            else 0
        )
        selected_grade = st.selectbox(
            "Select Grade Level",
            options=available_grades,
            index=current_index,
        )

        # Handle grade change — no DB reload needed
        if selected_grade != st.session_state.grade_level:
            st.session_state.grade_level = selected_grade
            st.session_state.correct_count = 0
            st.session_state.total_count = 0
            select_random_problem()
            st.rerun()

        st.divider()

        # Score display
        st.header("Score")
        st.metric("Correct", f"{st.session_state.correct_count} / {st.session_state.total_count}")

        if st.session_state.total_count > 0:
            accuracy = (st.session_state.correct_count / st.session_state.total_count) * 100
            st.progress(accuracy / 100, text=f"{accuracy:.0f}% accuracy")

        grade = st.session_state.grade_level
        remaining = len(st.session_state.problems.get(grade, []))
        st.metric("Problems Remaining", remaining)

        st.divider()

        if st.button("Reset Score", use_container_width=True):
            st.session_state.correct_count = 0
            st.session_state.total_count = 0
            st.rerun()

    # Select first problem if needed
    if st.session_state.current_problem is None:
        select_random_problem()

    if st.session_state.current_problem is None:
        st.warning(f"No problems found for grade {st.session_state.grade_level}.")
        return

    # Display current problem
    problem = st.session_state.current_problem

    st.subheader(f"Standard: {problem.get('standard', problem.get('standard_code', 'Unknown'))}")

    st.markdown("---")
    st.markdown(f"### {problem['problem']}")
    st.markdown("---")

    # Answer input and submission
    if not st.session_state.answered:
        with st.form("answer_form"):
            choices = st.session_state.current_problem.get("choices", [])
            user_answer = st.radio("Select your answer:", options=choices, index=None, key="answer_input")
            submitted = st.form_submit_button("Submit", use_container_width=True)

            if submitted and user_answer:
                check_answer(user_answer)
                st.rerun()
    else:
        # Show result
        if st.session_state.is_correct:
            st.success("Correct! Great job!")
        else:
            st.error(f"Incorrect. \nYour answer: **{st.session_state.user_answer}** \nCorrect answer: **{problem['solution']}**")

        if HUMAN_EVAL_MODE:
            st.markdown("---")
            st.subheader("Problem Evaluation")

            # Bad problem checkboxes
            st.write("If this is a bad problem, select the reason(s):")
            bad_answer_matching = st.checkbox("Bad answer matching")
            bad_format = st.checkbox("Bad format")
            incorrect_answer = st.checkbox("Incorrect answer")
            ambiguous = st.checkbox("Ambiguous question")
            missing_info = st.checkbox("Missing info")
            answer_in_q = st.checkbox("Answer in question")
            open_ended = st.checkbox("Question too open-ended")
            needs_graphic = st.checkbox("Needs graphic")
            unanswerable = st.checkbox("Unanswerable in text form")
            other = st.checkbox("Other")

            comments = ""
            comments = st.text_input("Additional comments:")

            bad_reasons = {
                "bad_answer_matching": bad_answer_matching,
                "bad_format": bad_format,
                "incorrect_answer": incorrect_answer,
                "ambiguous": ambiguous,
                "missing_info": missing_info,
                "answer_in_q": answer_in_q,
                "open_ended": open_ended,
                "needs_graphic": needs_graphic,
                "unanswerable": unanswerable,
                "other": other,
            }

            col1, col2 = st.columns(2)

            with col1:
                if st.button("Bad Problem", use_container_width=True):
                    write_log_entry(bad_problem=True, bad_reasons=bad_reasons, comments=comments)
                    select_random_problem()
                    st.rerun()

            with col2:
                if st.button("Good Problem", use_container_width=True):
                    write_log_entry(bad_problem=False, bad_reasons=bad_reasons, comments=comments)
                    select_random_problem()
                    st.rerun()
        else:
            if st.button("Next Problem", use_container_width=True, type="primary"):
                select_random_problem()
                st.rerun()


if __name__ == "__main__":
    main()
