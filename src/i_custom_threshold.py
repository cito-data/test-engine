from dataclasses import dataclass


@dataclass
class ForcedThreshold:
    value: float
    mode: str
