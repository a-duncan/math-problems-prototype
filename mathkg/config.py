"""Configuration dataclasses for the math-ed-kg pipeline."""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Neo4jConfig:
    uri: str = ""
    username: str = ""
    password: str = ""
    jurisdiction: str = "Multi-State"

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        return cls(
            uri=os.getenv("NEO4J_URI", ""),
            username=os.getenv("NEO4J_USERNAME", ""),
            password=os.getenv("NEO4J_PASSWORD", ""),
        )


@dataclass
class OllamaConfig:
    host: str = ""
    model: str = ""
    temperature: float = 0.0

    @classmethod
    def from_env(cls) -> "OllamaConfig":
        return cls(
            host=os.getenv("OLLAMA_HOST", ""),
            model=os.getenv("OLLAMA_MODEL", ""),
        )


@dataclass
class StoreConfig:
    db_path: Path = Path("data/mathkg.db")


@dataclass
class GenerationConfig:
    grade_levels: list[str] = field(default_factory=lambda: ["1", "2", "3", "4", "5", "6"])
    num_problems: int = 20
    batch_name: str | None = None
    verbose: bool = False


@dataclass
class EvalConfig:
    batch_name: str = "batch003"
    eval_batch_size: int = 5
    eval_run_label: str | None = None


@dataclass
class PracticeConfig:
    batch_name: str = "batch003"
    probs_per_std: int = 2
    problem_list: str | None = None  # filename under data/
    db_path: Path = Path("data/mathkg.db")


@dataclass
class HumanEvalConfig:
    source_batch_name: str = "batch003"
    source_eval_run_label: str | None = None  # auto-eval run to compare against
    probs_per_std: int | None = None           # limit per standard (None = all)
    grades: list[str] | None = None            # grade filter (None = all)
    eval_run_label: str | None = None          # label for this human eval run
    db_path: Path = Path("data/mathkg.db")


@dataclass
class LoadStandardsConfig:
    data_path: Path = Path("data")
    batch_size: int = 1000
    clear_graph: bool = True
    load_kg: bool = True

    @property
    def sf_path(self) -> str:
        return str(self.data_path / "MathStandardsFramework.csv")

    @property
    def sfi_path(self) -> str:
        return str(self.data_path / "MathStandardsFrameworkItem.csv")

    @property
    def lc_path(self) -> str:
        return str(self.data_path / "MathLearningComponent.csv")

    @property
    def relationships_path(self) -> str:
        return str(self.data_path / "MathRelationships.csv")
