from enum import Enum
from typing import Any, Tuple

class MaterializationType(Enum):
  TESTS = 'tests'
  EXECUTIONS = 'executions'
  TEST_HISTORY = 'test_history'
  TEST_RESULTS = 'test_results'
  ALERTS = 'alerts'

def getInsertQuery(valueSets: list[dict[str, Any]], type: MaterializationType):
  valueString = ', '.join(f"'{str(set.value)}'" if set.value else 'NULL' for set in valueSets)

  return f"""
  insert into cito.public.{type.value}
  values
  ({valueString});"""

def getHistoryQuery(testId: str):
  return f""" select value from cito.public.test_history
    where test_id = {testId} not is_anomaly or user_feedback_is_anomaly = 0;
  """

def getTestQuery(testId: str):
  return f""" select * from cito.public.tests
  where id = {testId};
  """