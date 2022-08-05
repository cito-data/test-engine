from enum import Enum
from typing import Any, Tuple

class CitoTableType(Enum):
  TestSuites = 'test_suites'
  Executions = 'executions'
  TestHistory = 'test_history'
  TestResults = 'test_results'
  Alerts = 'alerts'

def getInsertQuery(valueSets: list[dict[str, Any]], type: CitoTableType):
  valueString = ', '.join(f"'{str(set.value)}'" if set.value else 'NULL' for set in valueSets)

  return f"""
  insert into cito.public.{type.value}
  values
  ({valueString});"""

def getHistoryQuery(testId: str):
  return f""" select value from cito.public.test_history
    where test_id = '{testId}' not is_anomaly or user_feedback_is_anomaly = 0;
  """

def getTestQuery(testId: str):
  return f""" select * from cito.public.test_suites
  where id = '{testId}';
  """