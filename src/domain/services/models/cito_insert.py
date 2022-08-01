from enum import Enum
from typing import Any, Tuple

class MaterializationType(Enum):
  TESTS = 'tests'
  TEST_HISTORY = 'test_history'
  TEST_RESULTS = 'test_results'
  ALERTS = 'alerts'

def getInsert(valueSets: list[Any], type: MaterializationType):
  valueString = ', '.join(f"'{str(valueSet)}'" if valueSet else 'NULL' for valueSet in valueSets)

  return f"""
  insert into cito.public.{type.value}
  values
  ({valueString});"""