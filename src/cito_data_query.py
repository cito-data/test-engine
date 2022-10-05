from enum import Enum
from typing import Any

from .test_type import TestType

class CitoTableType(Enum):
  TestSuites = 'test_suites'
  TestHistory = 'test_history'
  TestResults = 'test_results'
  TestExecutions = 'test_executions'
  TestAlerts = 'test_alerts'
  TestSuitesNominal = 'test_suites_nominal'
  TestHistoryNominal = 'test_history_nominal'
  TestResultsNominal = 'test_results_nominal'
  TestExecutionsNominal = 'test_executions_nominal'
  TestAlertsNominal = 'test_alerts_nominal'

def getInsertQuery(valueSets: list[dict[str, Any]], type: CitoTableType):
  valueString = ', '.join(f"'{str(set['value'])}'" if set['value'] or set['value'] == 0 else 'NULL' for set in valueSets)

  return f"""
  insert into cito.observability.{type.value}
  values
  ({valueString});"""

def getHistoryQuery(testSuiteId: str):
  return f""" select value from cito.observability.test_history
    where test_suite_id = '{testSuiteId}' and (not is_anomaly or user_feedback_is_anomaly = 0);
  """

def getLastMatSchemaQuery(testSuiteId: str):
  return f"""
  with
  execution_id_cte as (select id from cito.observability.{CitoTableType.TestExecutionsNominal.value} where test_suite_id = '{testSuiteId}' order by executed_on limit 1)
  select execution_id_cte.id, test_history_nominal.value from execution_id_cte join (select value, execution_id from cito.observability.{CitoTableType.TestHistoryNominal.value}) as test_history_nominal
  on execution_id_cte.id = test_history_nominal.execution_id
  """

def getTestQuery(testSuiteId: str, testType: TestType):
  return f""" select * from cito.observability.{CitoTableType.TestSuites.value if testType == TestType.Anomaly else CitoTableType.TestSuitesNominal.value}
  where id = '{testSuiteId}';
  """