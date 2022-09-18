from enum import Enum
from typing import Any, Tuple

class CitoTableType(Enum):
  TestSuites = 'test_suites'
  Executions = 'executions'
  TestHistory = 'test_history'
  TestResults = 'test_results'
  Alerts = 'alerts'

def getInsertQuery(valueSets: list[dict[str, Any]], type: CitoTableType):
  valueString = ', '.join(f"'{str(set['value'])}'" if set['value'] or set['value'] == 0 else 'NULL' for set in valueSets)

  return f"""
  insert into cito.public.{type.value}
  values
  ({valueString});"""

def getHistoryQuery(testSuiteId: str):
  return f""" select value from cito.public.test_history
    where test_suite_id = '{testSuiteId}' and (not is_anomaly or user_feedback_is_anomaly = 0);
  """

def getLastMatSchemaQuery(testSuiteId: str):
  return f"""
  with
  execution_id_cte as (select id from cito.public.executions where test_suite_id = '{testSuiteId}' order by executed_on limit 1)
  select execution_id_cte.id, test_history.value from execution_id_cte join (select value, execution_id from cito.public.test_history) as test_history
  on execution_id_cte.id = test_history.execution_id
  """

def getTestQuery(testSuiteId: str):
  return f""" select * from cito.public.test_suites
  where id = '{testSuiteId}';
  """