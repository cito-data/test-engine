from dataclasses import dataclass
from typing import Any


@dataclass
class TestResultDto:
  result:list[dict[str, Any]]