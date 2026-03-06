"""
Math Practice App - Streamlit application for practicing grade-level math problems.
"""

import argparse
import sys

import streamlit as st

from mathkg.config import PracticeConfig
from mathkg.practice import PracticeSession, check_answer
from mathkg.store import ProblemStore


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-name", type=str, default="batch003",
                        help="Batch name to load problems from")
    parser.add_argument("--probs-per-std", type=int, default=2,
                        help="Number of problems per standard to load")
    parser.add_argument("--problem-list", type=str, default="batch003-good.txt",
                        help="Filename under data/ with one problem ID per line")
    return parser.parse_args(sys.argv[1:])


_args = _parse_args()

CONFIG = PracticeConfig(
    batch_name=_args.batch_name,
    probs_per_std=_args.probs_per_std,
    problem_list=_args.problem_list,
)


def _get_session() -> PracticeSession:
    """Get or create a PracticeSession in session state."""
    if "session" not in st.session_state:
        try:
            store = ProblemStore(CONFIG.db_path)
            st.session_state.session = PracticeSession(store, CONFIG)
        except ValueError as e:
            st.error(str(e))
            st.stop()
    return st.session_state.session


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


def select_next_problem(session: PracticeSession):
    """Select the next problem from the current grade's problem pool."""
    grade = st.session_state.grade_level
    if not grade:
        return

    problem = session.get_next_problem(grade)
    if problem is not None:
        st.session_state.current_problem = problem
        st.session_state.answered = False
        st.session_state.is_correct = None
        st.session_state.user_answer = ""
    else:
        st.session_state.current_problem = None


def main():
    session = _get_session()

    st.set_page_config(page_title="Math Practice", page_icon="\U0001f522", layout="centered")

    st.title("Math Practice")

    init_session_state()

    # Preload problems on first run
    if "problems_loaded" not in st.session_state:
        try:
            result = session.load_problems()
        except (FileNotFoundError, ValueError) as e:
            st.error(str(e))
            st.stop()
        if result.missing_ids:
            st.warning(f"{len(result.missing_ids)} problem ID(s) not found in database.")
        st.session_state.problems_loaded = True

    available_grades = session.available_grades

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

        # Handle grade change
        if selected_grade != st.session_state.grade_level:
            st.session_state.grade_level = selected_grade
            st.session_state.correct_count = 0
            st.session_state.total_count = 0
            select_next_problem(session)
            st.rerun()

        st.divider()

        # Score display
        st.header("Score")
        st.metric("Correct", f"{st.session_state.correct_count} / {st.session_state.total_count}")

        if st.session_state.total_count > 0:
            accuracy = (st.session_state.correct_count / st.session_state.total_count) * 100
            st.progress(accuracy / 100, text=f"{accuracy:.0f}% accuracy")

        grade = st.session_state.grade_level
        remaining = session.get_remaining_count(grade)
        st.metric("Problems Remaining", remaining)

        st.divider()

        if st.button("Reset Score", use_container_width=True):
            st.session_state.correct_count = 0
            st.session_state.total_count = 0
            st.rerun()

    # Select first problem if needed
    if st.session_state.current_problem is None:
        select_next_problem(session)

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
        choices = st.session_state.current_problem.get("choices", [])
        max_len = max((len(c) for c in choices), default=0)

        def _handle_choice(choice):
            is_correct = check_answer(choice, problem["solution"])
            st.session_state.answered = True
            st.session_state.total_count += 1
            st.session_state.user_answer = choice
            st.session_state.is_correct = is_correct
            if is_correct:
                st.session_state.correct_count += 1

        if max_len <= 40 and len(choices) == 4:
            # 2x2 grid
            for row_start in (0, 2):
                cols = st.columns(2)
                for i, col in enumerate(cols):
                    idx = row_start + i
                    with col:
                        if st.button(choices[idx], key=f"choice_{idx}", use_container_width=True):
                            _handle_choice(choices[idx])
                            st.rerun()
        elif max_len <= 40 and len(choices) == 2:
            # 1x2 row
            cols = st.columns(2)
            for i, col in enumerate(cols):
                with col:
                    if st.button(choices[i], key=f"choice_{i}", use_container_width=True):
                        _handle_choice(choices[i])
                        st.rerun()
        else:
            # Single column
            for i, choice in enumerate(choices):
                if st.button(choice, key=f"choice_{i}", use_container_width=True):
                    _handle_choice(choice)
                    st.rerun()
    else:
        # Show result
        if st.session_state.is_correct:
            st.success(f"Correct! Great job! \nYour answer: **{st.session_state.user_answer}**")
        else:
            st.error(f"Incorrect. \nYour answer: **{st.session_state.user_answer}** \nCorrect answer: **{problem['solution']}**")

        if st.button("Next Problem", use_container_width=True, type="primary"):
            select_next_problem(session)
            st.rerun()


if __name__ == "__main__":
    main()
