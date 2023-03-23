from enum import Enum
from dataclasses import dataclass


class ForcedThresholdMode(Enum):
    ABSOLUTE = 'absolute'
    RELATIVE = 'relative'


class ForcedThresholdType(Enum):
    FEEDBACK = 'feedback'
    CUSTOM = 'custom'


@dataclass
class ForcedThreshold:
    value: float
    mode: ForcedThresholdMode
    type: ForcedThresholdType
