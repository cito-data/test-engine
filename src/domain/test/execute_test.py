from dataclasses import dataclass
import json
from typing import Union
from unittest import case
import jwt
from domain.services.models.cito_insert import MaterializationType, getInsert
from domain.services.models.new_data_query import getRowCount
from domain.value_types.statistical_model import AnomalyMessage, ModelType, ResultDto, RowCountModel
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto
from src.domain.services.use_case import IUseCase
from src.domain.integration_api.i_integration_api_repo import IIntegrationApiRepo
import logging
import datetime
import uuid

from src.domain.value_types.transient_types.result import Result

logger = logging.getLogger(__name__)


@dataclass
class ExecuteTestRequestDto:
    executionId: str
    materializationAddress: str
    columnName: Union[str, None]
    testType: str
    threshold: int


@dataclass
class ExecuteTestAuthDto:
    jwt: str
    organizationId: str


ExecuteTestResponseDto = Result[ResultDto]


class ExecuteTest(IUseCase):

    def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
        self._integrationApiRepo = integrationApiRepo
        self._querySnowflake = querySnowflake

    def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
        try:

            newDataQuery = getRowCount(
                'snowflake_sample_data.information_schema.tables', 'tpcds_sf100tcl', 'warehouse')
            newDataQueryResult = self._querySnowflake.execute(
                QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(jwt))

            # todo - filter for non anomaly history values
            # todo - what if empty
            # what if mat does not exist
            historyDataQueryResult = self._querySnowflake.execute(
                QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(auth.jwt))

            if not newDataQueryResult.success:
                raise Exception(newDataQueryResult.error)
            if not newDataQueryResult.value:
                raise Exception('No new data received')

            result = None
            anomalyMessage = None
            if request.testType == ModelType.ROW_COUNT.value:
                result = RowCountModel(
                    [1, 2, 3], [100, 101, 102, 103], request.threshold).run()
                anomalyMessage = AnomalyMessage.ROW_COUNT.value
            else:
                raise Exception('Test type mismatch')
            """ 
            {
              name: 'tests',
              columns: [
                { name: 'id', type: 'string' },
                { name: 'test_type', type: 'string' },
                { name: 'threshold', type: 'integer' },
                { name: 'materialization_address', type: 'string' },
                { name: 'column_name', type: 'string' },
                { name: 'executed_on', type: 'datetime' },
                { name: 'execution_id', type: 'string' },
              ],
            },
            """
            testType = result.type
            executionId = request.executionId

            testId = str(uuid.uuid4())
            threshold = result.threshold
            materializationAddress = request.materializationAddress
            columnName = request.columnName
            executedOn = datetime.datetime.now().isoformat()

            testsQuery = getInsert([testId, testType, threshold, materializationAddress,
                                   columnName, executedOn, executionId], MaterializationType.TESTS)
            testsQueryResult = self._querySnowflake.execute(
                QuerySnowflakeRequestDto(testsQuery), QuerySnowflakeAuthDto(auth.jwt))

            """     {
            name: 'test_history',
            columns: [
              { name: 'id', type: 'string' },
              { name: 'test_type', type: 'string' },
              { name: 'value', type: 'float' },
              { name: 'is_anomaly', type: 'boolean' },
              { name: 'user_anomaly_feedback', type: 'string' },
              { name: 'execution_id', type: 'string' },
            ],
            },
            """
            testHistoryId = str(uuid.uuid4())
            value = result.newDatapoint
            isAnomaly = result.isAnomaly
            userAnomalyFeedback = None

            testHistoryQuery = getInsert([testHistoryId, testType, value, isAnomaly,
                                         userAnomalyFeedback, executionId], MaterializationType.TEST_HISTORY)
            testHistoryResult = self._querySnowflake.execute(
                QuerySnowflakeRequestDto(testHistoryQuery), QuerySnowflakeAuthDto(auth.jwt))

            """     {
            name: 'test_results',
            columns: [
              { name: 'id', type: 'string' },
              { name: 'test_type', type: 'string' },
              { name: 'mean_ad', type: 'float' },
              { name: 'median_ad', type: 'float' },
              { name: 'modified_z_score', type: 'float' },
              { name: 'expected_value', type: 'float' },
              { name: 'expected_value_upper_bound', type: 'float' },
              { name: 'expected_value_lower_bound', type: 'float' },
              { name: 'deviation', type: 'float' },
              { name: 'is_anomalous', type: 'boolean' },
              { name: 'execution_id', type: 'string' },
            ],
            }, 
            """

            testResultId = str(uuid.uuid4())
            meanAd = result.meanAbsoluteDeviation
            medianAd = result.medianAbsoluteDeviation
            modifiedZScore = result.modifiedZScore
            expectedValue = result.expectedValue
            expectedValueUpperBound = result.expectedValueUpperBoundary
            expectedValueLowerBound = result.expectedValueLowerBoundary
            deviation = result.deviation
            isAnomalous = result.isAnomaly

            testResultQuery = getInsert([testResultId, testType, meanAd, medianAd, modifiedZScore, expectedValue, expectedValueUpperBound,
                                        expectedValueLowerBound, deviation, isAnomalous, executionId], MaterializationType.TEST_RESULTS)
            testResultResult = self._querySnowflake.execute(
                QuerySnowflakeRequestDto(testResultQuery), QuerySnowflakeAuthDto(auth.jwt))

            if result.isAnomaly:
                """ {
                name: 'alerts',
                columns: [
                  { name: 'id', type: 'string' },
                  { name: 'test_type', type: 'string' },
                  { name: 'message', type: 'string' },
                  { name: 'execution_id', type: 'string' },
                ],
                },
                """

                alertId = str(uuid.uuid4())

                testAlertQuery = getInsert(
                    [alertId, testType, anomalyMessage, executionId], MaterializationType.ALERTS)
                testAlertResult = self._querySnowflake.execute(
                    QuerySnowflakeRequestDto(testAlertQuery), QuerySnowflakeAuthDto(auth.jwt))

            return Result.ok(result)

        except Exception as e:
            logger.error(e)
            return Result.fail(e)
