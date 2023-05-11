from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import json
from typing import Any, Union
from cito_data_query import CitoTableType, insertTableData, getTestData, getHistoryData, getLastMatSchemaData, updateTableData
from mongo_db import get_mongo_connection
from new_column_data_query import getCardinalityQuery, getDistributionQuery, getNullnessQuery, getUniquenessQuery, getFreshnessQuery as getColumnFreshnessQuery
from new_materialization_data_query import MaterializationType, getColumnCountQuery, getFreshnessQuery, getRowCountQuery, getSchemaChangeQuery
from qual_model import ColumnDefinition, SchemaChangeModel, ResultDto as QualResultDto
from quant_model import ResultDto as QuantTestResultDto, CommonModel
from query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto, QuerySnowflakeResponseDto
from i_forced_threshold import ForcedThreshold, ForcedThresholdMode, ForcedThresholdType
from test_execution_result import QualTestAlertData, QualTestData, QualTestExecutionResult, QuantTestAlertData, QuantTestData, QuantTestExecutionResult, AnomalyData
from test_type import QuantColumnTest, QuantMatTest, QualMatTest
from use_case import IUseCase
import logging
import uuid

from result import Result

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def getAnomalyMessage(targetResourceId: str, databaseName: str, schemaName: str, materializationName: str, columnName: Union[str, None], testType: str):
    targetResourceUrlTemplate = f'__base_url__?targetResourceId={targetResourceId}&ampisColumn={not not columnName}'

    if (testType == QuantColumnTest.ColumnFreshness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnDistribution.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnCardinality.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnNullness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnUniqueness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantMatTest.MaterializationColumnCount.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantMatTest.MaterializationRowCount.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantMatTest.MaterializationFreshness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QualMatTest.MaterializationSchemaChange.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    else:
        raise Exception('Unhandled anomaly message test type')


@dataclass
class ExecuteTestRequestDto:
    testSuiteId: str
    testType: Union[QuantColumnTest, QuantMatTest, QualMatTest]
    targetOrgId: Union[str, None]


@dataclass
class ExecuteTestAuthDto:
    jwt: str
    callerOrgId: Union[str, None]
    isSystemInternal: bool


ExecuteTestResponseDto = Result[Union[QuantTestExecutionResult,
                                      QualTestExecutionResult]]


class ExecuteTest(IUseCase):

    _MIN_HISTORICAL_DATA_TEST_NUMBER_CONDITION = 10
    _MIN_HISTORICAL_DATA_DAY_NUMBER_CONDITION = 7

    _testSuiteId: str
    _testType: Union[QuantColumnTest, QuantMatTest, QualMatTest]
    _testDefinition: "dict[str, Any]"

    _targetOrgId: Union[str, None]
    _organizationId: str

    _executionId: str
    _jwt: str

    _requestLoggingInfo: str

    _querySnowflake: QuerySnowflake

    def __init__(self, querySnowflake: QuerySnowflake) -> None:
        self._querySnowflake = querySnowflake
        self._dbConnection = get_mongo_connection()

    def _insertExecutionEntry(self, executedOn: str, tableType: CitoTableType):

        doc = {
            'id': self._executionId,
            'executed_on': executedOn,
            'test_suite_id': self._testSuiteId
        }

        insertTableData(doc, tableType, self._dbConnection,
                        self._organizationId)

    def _insertQualHistoryEntry(self, value: "dict[str, ColumnDefinition]", isIdentical: bool, alertId: Union[str, None]):

        doc = {
            'id': str(uuid.uuid4()),
            'test_type': self._testDefinition['test_type'],
            'is_identical': isIdentical,
            'test_suite_id': self._testSuiteId,
            'execution_id': self._executionId,
            'alert_id': alertId
        }

        insertTableData(doc, CitoTableType.TestHistoryQual,
                        self._dbConnection, self._organizationId)

    def _insertHistoryEntry(self, value: str, isAnomaly: bool, alertId: Union[str, None]):

        doc = {
            'id': str(uuid.uuid4()),
            'test_type': self._testDefinition['test_type'],
            'value': value,
            'is_anomaly': isAnomaly,
            'user_feedback_is_anomaly': -1,
            'test_suite_id': self._testSuiteId,
            'execution_id': self._executionId,
            'alert_id': alertId
        }

        insertTableData(doc, CitoTableType.TestHistory,
                        self._dbConnection, self._organizationId)

    def _insertQualTestResultEntry(self, testResult: QualResultDto):

        doc = {
            'id': str(uuid.uuid4()),
            'test_type': self._testDefinition['test_type'],
            'expected_value': json.dumps(testResult.expectedValue) if testResult.expectedValue else None,
            'deviation': json.dumps([asdict(el) for el in testResult.deviations]),
            'is_identical': testResult.isIdentical,
            'test_suite_id': self._testSuiteId,
            'execution_id': self._executionId
        }

        insertTableData(doc, CitoTableType.TestResultsQual,
                        self._dbConnection, self._organizationId)

    def _insertResultEntry(self, testResult: QuantTestResultDto):

        doc = {
            'id': str(uuid.uuid4()),
            'test_type': self._testDefinition['test_type'],
            'mean_ad': testResult.meanAbsoluteDeviation,
            'median_ad': testResult.medianAbsoluteDeviation,
            'modified_z_score': testResult.modifiedZScore,
            'expected_value': testResult.expectedValue,
            'expected_value_upper_bound': testResult.expectedValueUpper,
            'expected_value_lower_bound': testResult.expectedValueLower,
            'deviation': testResult.deviation,
            'is_anomalous': bool(testResult.anomaly),
            'test_suite_id': self._testSuiteId,
            'execution_id': self._executionId,
            'importance': testResult.anomaly.importance if testResult.anomaly else None
        }

        insertTableData(doc, CitoTableType.TestResults,
                        self._dbConnection, self._organizationId)

    def _insertAlertEntry(self, id, message: str, tableType: CitoTableType):

        doc = {
            'id': id,
            'test_type': self._testDefinition['test_type'],
            'message': message,
            'test_suite_id': self._testSuiteId,
            'execution_id': self._executionId
        }

        insertTableData(doc, tableType, self._dbConnection,
                        self._organizationId)

    def _getTestEntry(self) -> Any:

        return getTestData(self._testSuiteId, self._testType, self._dbConnection, self._organizationId)

    def _fromIsoFormatToDateTime(self, dateTime: str) -> datetime:
        return datetime.fromisoformat(dateTime.split('.')[0])

    def _toProphetDtFormat(self, dt: datetime) -> str:
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def _getHistoricalData(self) -> "list[tuple[str, float]]":

        historyData = getHistoryData(
            self._testSuiteId, self._dbConnection, self._organizationId)

        return sorted([(self._toProphetDtFormat(self._fromIsoFormatToDateTime(element['executed_on'])), element['value']) for element in historyData])

    def _getLastMatSchema(self) -> Union["dict[str, ColumnDefinition]", None]:

        result = getLastMatSchemaData(
            self._testSuiteId, self._dbConnection, self._organizationId)

        return (json.loads(result[0]['value']) if len(result) else None)

    def _getNewData(self, query) -> "list[dict[str, Any]]":
        getNewDataResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not getNewDataResult.success:
            raise Exception(getNewDataResult.error)
        if not getNewDataResult.value:
            raise Exception('Sf query error - operation: new data')

        newData = getNewDataResult.value.content[self._organizationId]

        return newData

    def _updateLastAlertSent(self, lastAlertSent: str, isQualTest: bool):
        tableType = CitoTableType.TestSuitesQual if isQualTest else CitoTableType.TestSuites

        updateTableData(self._testSuiteId, tableType, 'last_alert_sent',
                        lastAlertSent, self._dbConnection, self._organizationId)

    def _calculateLastAlertSent(self, lastAlertSent: str, isQualTest: bool):
        if not lastAlertSent:
            lastAlertSent = datetime.utcnow().isoformat()
            self._updateLastAlertSent(lastAlertSent, isQualTest=isQualTest)
        else:
            lastAlertSentDt = self._fromIsoFormatToDateTime(lastAlertSent)
            currTime = datetime.utcnow()
            diff = currTime - lastAlertSentDt
            if diff >= timedelta(hours=24):
                currTimeISO = currTime.isoformat()
                self._updateLastAlertSent(currTimeISO, isQualTest=isQualTest)

        return lastAlertSent

    def _runModel(self, newData: "tuple[str, float]", historicalData: "list[tuple[str, float]]", testType: Union[QuantMatTest, QuantColumnTest], forcedLowerThreshold: "Union[ForcedThreshold, None]", forcedUpperThreshold: "Union[ForcedThreshold, None]", ) -> QuantTestResultDto:
        return CommonModel(newData, historicalData, testType, forcedLowerThreshold, forcedUpperThreshold, ).run()

    def _runTest(self, newDataPoint, historicalData: "list[tuple[str,float]]") -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        materializationType = self._testDefinition['materialization_type']
        columnName = self._testDefinition['column_name']
        testSuiteId = self._testDefinition['id']
        customLowerThreshold = self._testDefinition['custom_lower_threshold']
        customLowerThresholdMode = self._testDefinition['custom_lower_threshold_mode']
        customUpperThreshold = self._testDefinition['custom_upper_threshold']
        customUpperThresholdMode = self._testDefinition['custom_upper_threshold_mode']
        targetResourceId = self._testDefinition['target_resource_id']
        testType = self._testDefinition['test_type']
        feedbackLowerThreshold = self._testDefinition['feedback_lower_threshold']
        feedbackUpperThreshold = self._testDefinition['feedback_upper_threshold']
        lastAlertSent = self._testDefinition['last_alert_sent']

        executedOn = datetime.utcnow()
        executedOnISOFormat = executedOn.isoformat()

        self._insertExecutionEntry(
            executedOnISOFormat, CitoTableType.TestExecutions)

        historicalDataLength = len(historicalData)
        belowDayBoundary = True if historicalDataLength == 0 else (
            executedOn - self._fromIsoFormatToDateTime(historicalData[0][0])).days <= self._MIN_HISTORICAL_DATA_DAY_NUMBER_CONDITION
        if (belowDayBoundary or historicalDataLength <= self._MIN_HISTORICAL_DATA_TEST_NUMBER_CONDITION):
            self._insertHistoryEntry(
                newDataPoint, False, None)

            return QuantTestExecutionResult(testSuiteId, testType, self._executionId, targetResourceId, self._organizationId, True, None, None, lastAlertSent)

        lowerThreshold = None if feedbackLowerThreshold is None else ForcedThreshold(
            feedbackLowerThreshold, ForcedThresholdMode.ABSOLUTE, ForcedThresholdType.FEEDBACK)
        upperThreshold = None if feedbackUpperThreshold is None else ForcedThreshold(
            feedbackUpperThreshold, ForcedThresholdMode.ABSOLUTE, ForcedThresholdType.FEEDBACK)

        forcedLowerThresholdMode = ForcedThresholdMode.ABSOLUTE
        if customLowerThresholdMode == ForcedThresholdMode.RELATIVE.value:
            forcedLowerThresholdMode = ForcedThresholdMode.RELATIVE
        elif customLowerThresholdMode != ForcedThresholdMode.ABSOLUTE.value:
            raise Exception('Invalid custom lower threshold mode')

        lowerThreshold = lowerThreshold if customLowerThreshold is None else ForcedThreshold(
            customLowerThreshold, forcedLowerThresholdMode, ForcedThresholdType.CUSTOM)

        forcedUpperThresholdMode = ForcedThresholdMode.ABSOLUTE
        if customUpperThresholdMode == ForcedThresholdMode.RELATIVE.value:
            forcedUpperThresholdMode = ForcedThresholdMode.RELATIVE
        elif customUpperThresholdMode != ForcedThresholdMode.ABSOLUTE.value:
            raise Exception('Invalid custom upper threshold mode')

        upperThreshold = upperThreshold if customUpperThreshold is None else ForcedThreshold(
            customUpperThreshold, forcedUpperThresholdMode, ForcedThresholdType.CUSTOM)

        testResult = self._runModel(
            (executedOnISOFormat, newDataPoint), historicalData, testType, lowerThreshold, upperThreshold)

        self._insertResultEntry(testResult)

        alertData = None
        alertId = None
        if testResult.anomaly:
            print('Anomaly detected for test suite: ' + testSuiteId +
                  ' and organization: ' + self._organizationId)
            anomalyMessage = getAnomalyMessage(
                targetResourceId, databaseName, schemaName, materializationName, columnName, testType)
            alertId = str(uuid.uuid4())
            self._insertAlertEntry(
                alertId, anomalyMessage, CitoTableType.TestAlerts)

            alertData = QuantTestAlertData(alertId, anomalyMessage, databaseName, schemaName,
                                           materializationName, materializationType, testResult.expectedValue, columnName)

            lastAlertSent = self._calculateLastAlertSent(
                lastAlertSent, isQualTest=False)

        testData = QuantTestData(
            executedOnISOFormat, newDataPoint, testResult.expectedValueUpper,
            testResult.expectedValueLower, testResult.modifiedZScore, testResult.deviation, AnomalyData(testResult.anomaly.importance) if testResult.anomaly else None)

        self._insertHistoryEntry(
            newDataPoint, bool(testResult.anomaly), alertId)

        return QuantTestExecutionResult(testSuiteId, testType, self._executionId, targetResourceId, self._organizationId, False, testData, alertData, lastAlertSent)

    def _runSchemaChangeModel(self, oldSchema: "dict[str, ColumnDefinition]", newSchema: "dict[str, ColumnDefinition]") -> QualResultDto:
        return SchemaChangeModel(newSchema, oldSchema).run()

    def _runSchemaChangeTest(self, oldSchema: "dict[str, ColumnDefinition]", newSchema: "dict[str, ColumnDefinition]") -> QualTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        materializationType = self._testDefinition['materialization_type']
        columnName = self._testDefinition['column_name']
        testSuiteId = self._testDefinition['id']
        testType = self._testDefinition['test_type']
        targetResourceId = self._testDefinition['target_resource_id']
        lastAlertSent = self._testDefinition['last_alert_sent']

        executedOn = datetime.utcnow().isoformat()

        testResult = self._runSchemaChangeModel(
            oldSchema, newSchema)

        self._insertExecutionEntry(
            executedOn, CitoTableType.TestExecutionsQual)

        self._insertQualTestResultEntry(testResult)

        alertData = None
        alertId = None
        if not testResult.isIdentical:
            anomalyMessage = getAnomalyMessage(
                targetResourceId, databaseName, schemaName, materializationName, columnName, self._testDefinition['test_type'])
            alertId = str(uuid.uuid4())
            self._insertAlertEntry(
                alertId, anomalyMessage, CitoTableType.TestAlertsQual)

            alertData = QualTestAlertData(alertId, anomalyMessage, databaseName, schemaName,
                                          materializationName, materializationType, testResult.deviations)

            lastAlertSent = self._calculateLastAlertSent(
                lastAlertSent, isQualTest=True)

        self._insertQualHistoryEntry(
            newSchema, testResult.isIdentical, alertId)

        testData = QualTestData(
            executedOn, testResult.deviations, testResult.isIdentical)
        return QualTestExecutionResult(testSuiteId, testType, self._executionId, targetResourceId, self._organizationId, testData, alertData, lastAlertSent)

    def _runMaterializationRowCountTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        materializationType = self._testDefinition['materialization_type']

        newDataQuery = getRowCountQuery(
            databaseName, schemaName, materializationName, MaterializationType[materializationType])

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                f'Mat row count - More than one or no matching new data entries found')

        newDataPoint = newData[0]['ROW_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationColumnCountTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']

        newDataQuery = getColumnCountQuery(
            databaseName, schemaName, materializationName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Mat column count - More than one or no matching new data entries found')

        newDataPoint = newData[0]['COLUMN_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationFreshnessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        materializationType = self._testDefinition['materialization_type']

        newDataQuery = getFreshnessQuery(
            databaseName, schemaName, materializationName, materializationType)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Mat freshness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationSchemaChangeTest(self) -> QualTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']

        newDataQuery = getSchemaChangeQuery(
            databaseName, schemaName, materializationName)

        newData = self._getNewData(newDataQuery)

        newSchema: dict[str, ColumnDefinition] = {}
        for el in newData:
            columnDefinition = el['COLUMN_DEFINITION']
            ordinalPosition = columnDefinition['ORDINAL_POSITION']
            newSchema[str(ordinalPosition)] = ColumnDefinition(columnDefinition['COLUMN_NAME'],  columnDefinition['DATA_TYPE'],
                                                               columnDefinition['IS_IDENTITY'],  columnDefinition['IS_NULLABLE'],  columnDefinition['ORDINAL_POSITION'])

        oldSchema = self._getLastMatSchema()

        if not oldSchema:
            raise Exception(
                'Cannot test for schema change. Missing old schema.')

        testResult = self._runSchemaChangeTest(oldSchema, newSchema)

        return testResult

    def _runColumnCardinalityTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        columnName = self._testDefinition['column_name']

        newDataQuery = getCardinalityQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col cardinality - More than one or no matching new data entries found')

        newDataPoint = newData[0]['DISTINCT_VALUE_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnDistributionTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        columnName = self._testDefinition['column_name']

        newDataQuery = getDistributionQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Distribution - More than one or no matching new data entries found')

        newDataPoint = newData[0]['MEDIAN']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnFreshnessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        columnName = self._testDefinition['column_name']

        newDataQuery = getColumnFreshnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Freshness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnNullnessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        columnName = self._testDefinition['column_name']

        newDataQuery = getNullnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Nullness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['NULLNESS_RATE']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnUniquenessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['database_name']
        schemaName = self._testDefinition['schema_name']
        materializationName = self._testDefinition['materialization_name']
        columnName = self._testDefinition['column_name']

        newDataQuery = getUniquenessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Uniqueness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['UNIQUENESS_RATE']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _getTestDefinition(self):
        getTestEntryResult = self._getTestEntry()

        organizationResult = [getTestEntryResult]
        if not len(organizationResult) == 1:
            raise Exception('Test Definition - More than one or no test found')

        return organizationResult[0]

    def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
        try:
            if not request.targetOrgId and not auth.callerOrgId:
                raise Exception('No organization Id instance provided')
            if request.targetOrgId and auth.callerOrgId:
                raise Exception(
                    'callerOrgId and targetOrgId provided. Not allowed')

            if auth.isSystemInternal:
                if not request.targetOrgId:
                    raise Exception('Target organization id missing')
                orgId = request.targetOrgId
            else:
                if not auth.callerOrgId:
                    raise Exception('Caller organization id missing')
                orgId = auth.callerOrgId

            self._testSuiteId = request.testSuiteId
            self._testType = request.testType
            self._targetOrgId = request.targetOrgId
            self._organizationId = orgId
            self._requestLoggingInfo = f'(organizationId: {self._organizationId}, testSuiteId: {self._testSuiteId}, testType: {self._testType})'
            self._executionId = str(uuid.uuid4())
            self._jwt = auth.jwt
            self._testDefinition = self._getTestDefinition()

            testTypeKey = 'test_type'

            if self._testDefinition[testTypeKey] == QuantMatTest.MaterializationRowCount.value:
                testResult = self._runMaterializationRowCountTest()
            elif self._testDefinition[testTypeKey] == QuantMatTest.MaterializationColumnCount.value:
                testResult = self._runMaterializationColumnCountTest()
            elif self._testDefinition[testTypeKey] == QuantMatTest.MaterializationFreshness.value:
                testResult = self._runMaterializationFreshnessTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnCardinality.value:
                testResult = self._runColumnCardinalityTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnDistribution.value:
                testResult = self._runColumnDistributionTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnFreshness.value:
                testResult = self._runColumnFreshnessTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnNullness.value:
                testResult = self._runColumnNullnessTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnUniqueness.value:
                testResult = self._runColumnUniquenessTest()
            elif self._testDefinition[testTypeKey] == QualMatTest.MaterializationSchemaChange.value:
                testResult = self._runMaterializationSchemaChangeTest()
            else:
                raise Exception('Test type mismatch')

            return Result.ok(testResult)

        except Exception as e:
            logger.exception(
                f'error: {e}' if e.args[0] else f'error: unknown - {self._requestLoggingInfo}')
            return Result.fail('')
