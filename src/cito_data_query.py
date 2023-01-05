from enum import Enum
from typing import Any, Union

from test_type import AnomalyColumnTest, AnomalyMatTest, QualMatTest

class CitoTableType(Enum):
  TestSuites = 'test_suites'
  TestHistory = 'test_history'
  TestResults = 'test_results'
  TestExecutions = 'test_executions'
  TestAlerts = 'test_alerts'
  TestSuitesQual = 'test_suites_qual'
  TestHistoryQual = 'test_history_qual'
  TestResultsQual = 'test_results_qual'
  TestExecutionsQual = 'test_executions_qual'
  TestAlertsQual = 'test_alerts_qual'

anomalyColumnTest = set(item.value for item in AnomalyColumnTest)
anomalyMatTest = set(item.value for item in AnomalyMatTest)
qualMatTest = set(item.value for item in QualMatTest)

def getInsertQuery(valueSets: "list[dict[str, Any]]", type: CitoTableType):
  valueString = ', '.join(f"'{str(set['value'])}'" if set['value'] or set['value'] == 0 else 'NULL' for set in valueSets)

  return f"""
  insert into cito.observability.{type.value}
  values
  ({valueString});"""

def getHistoryQuery(testSuiteId: str):
  return f""" select test_executions.executed_on as executed_on, test_history.value as value 
  from cito.observability.test_history as test_history
  inner join cito.observability.test_executions as test_executions
    on test_history.execution_id = test_executions.id
    where test_history.test_suite_id = '{testSuiteId}' and (not test_history.is_anomaly or test_history.user_feedback_is_anomaly = 0);
  """

def getLastMatSchemaQuery(testSuiteId: str):
  return f"""
  with
  execution_id_cte as (select id from cito.observability.{CitoTableType.TestExecutionsQual.value} where test_suite_id = '{testSuiteId}' order by executed_on desc limit 1)
  select execution_id_cte.id, test_history_qual.value from execution_id_cte join (select value, execution_id from cito.observability.{CitoTableType.TestHistoryQual.value}) as test_history_qual
  on execution_id_cte.id = test_history_qual.execution_id
  """

def getTestQuery(testSuiteId: str, testType: Union[AnomalyColumnTest, AnomalyMatTest, QualMatTest]):
  return f""" select * from cito.observability.{CitoTableType.TestSuites.value if testType in anomalyColumnTest or testType in anomalyMatTest else CitoTableType.TestSuitesQual.value}
  where id = '{testSuiteId}';
  """