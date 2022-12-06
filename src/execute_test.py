from dataclasses import dataclass, asdict
import datetime
import json
from typing import Any, Union
from numpy import integer
from cito_data_query import CitoTableType, getHistoryQuery, getInsertQuery, getTestQuery, getLastMatSchemaQuery
from new_column_data_query import getCardinalityQuery, getDistributionQuery, getNullnessQuery, getUniquenessQuery, getFreshnessQuery as getColumnFreshnessQuery
from new_materialization_data_query import MaterializationType, getColumnCountQuery, getFreshnessQuery, getRowCountQuery, getSchemaChangeQuery
from nominal_model import MaterializationSchema, SchemaChangeModel, ResultDto as NominalResultDto, SchemaDiff
from anomaly_model import ResultDto as AnomalyTestResultDto, CommonModel
from query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto, QuerySnowflakeResponseDto
from test_type import AnomalyColumnTest, AnomalyMatTest, NominalMatTest
from use_case import IUseCase
from i_integration_api_repo import IIntegrationApiRepo
import logging
import uuid

from result import Result

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def getAnomalyMessage(targetResourceId: str, databaseName: str, schemaName: str, materializationName: str, columnName: Union[str, None], testType: str):
    targetResourceUrlTemplate = f'__base_url__?targetResourceId={targetResourceId}&ampisColumn={not not columnName}'

    if(testType == AnomalyColumnTest.ColumnFreshness.value ):
        return f"Freshness deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == AnomalyColumnTest.ColumnDistribution.value ):
        return f"Distribution deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == AnomalyColumnTest.ColumnCardinality.value ):
        return f"Cardinality deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == AnomalyColumnTest.ColumnNullness.value ):
        return f"Nullness deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == AnomalyColumnTest.ColumnUniqueness.value ):
        return f"Uniqueness deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == AnomalyMatTest.MaterializationColumnCount.value ):
        return f"Column count deviation for materialization <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == AnomalyMatTest.MaterializationRowCount.value ):
        return f"Row count deviation for materialization <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == AnomalyMatTest.MaterializationFreshness.value ):
        return f"Freshness deviation for materialization <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == NominalMatTest.MaterializationSchemaChange.value ):
        return f"Schema change for materialization <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    else:
        raise Exception('Unhandled anomaly message test type')

@dataclass
class _TestData:
    executedOn: str

@dataclass
class AnomalyTestData(_TestData):
    isAnomolous: bool
    modifiedZScore: float
    deviation: float

@dataclass
class NominalTestData(_TestData):
    deviations: str
    isIdentical: bool

@dataclass
class _AlertData:
    alertId: str
    message: str
    databaseName: str
    schemaName: str
    materializationName: str
    materializationType: str

@dataclass
class AnomalyTestAlertData(_AlertData):
    expectedUpperBound: Union[float, None]
    expectedLowerBound: Union[float, None]
    columnName: Union[str, None]
    value: float

@dataclass
class NominalTestAlertData(_AlertData):
    deviatons: str

@dataclass
class _TestExecutionResult:
    testSuiteId: str
    testType: str
    executionId: str
    targetResourceId: str
    organizationId: str

@dataclass
class AnomalyTestExecutionResult(_TestExecutionResult):
    isWarmup: bool
    testData: Union[AnomalyTestData, None]
    alertData: Union[AnomalyTestAlertData, None]

@dataclass
class NominalTestExecutionResult(_TestExecutionResult):
    testData: NominalTestData
    alertData: Union[NominalTestAlertData, None]

@dataclass
class ExecuteTestRequestDto:
    testSuiteId: str
    testType: Union[AnomalyColumnTest, AnomalyMatTest, NominalMatTest] 
    targetOrgId: Union[str, None]

@dataclass
class ExecuteTestAuthDto:
    jwt: str
    callerOrgId: Union[str, None]
    isSystemInternal: bool
    
ExecuteTestResponseDto = Result[Union[AnomalyTestExecutionResult, NominalTestExecutionResult]]


class ExecuteTest(IUseCase):

    _MIN_HISTORICAL_DATA_NUMBER_TEST_CONDITION = 10

    _testSuiteId: str
    _testType: Union[AnomalyColumnTest, AnomalyMatTest, NominalMatTest]
    _testDefinition: "dict[str, Any]"

    _targetOrgId: str
    _organizationId: str

    _executionId: str
    _jwt: str

    _requestLoggingInfo: str

    def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
        self._integrationApiRepo = integrationApiRepo
        self._querySnowflake = querySnowflake

    def _insertExecutionEntry(self, executedOn: str, tableType: CitoTableType):
        valueSets = [
            {'name': 'id', 'value': self._executionId, 'type': 'string'},
            {'name': 'executedOn', 'value': executedOn, 'type': 'timestamp_ntz'},
            {'name': 'testSuiteId', 'value': self._testSuiteId, 'type': 'string'},
        ]

        executionQuery = getInsertQuery(
            valueSets, tableType)
        executionEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(executionQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not executionEntryInsertResult.success:
            raise Exception(executionEntryInsertResult.error)

    def _insertNominalHistoryEntry(self, value: MaterializationSchema, isIdentical: bool, alertId: Union[str, None]):
        valueSets = [
        { 'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
        { 'name': 'test_type', 'type': 'string', 'value': self._testDefinition['TEST_TYPE']},
        { 'name': 'value', 'type': 'string', 'value':  json.dumps(value)},
        { 'name': 'is_identical', 'type': 'boolean', 'value': 'true' if isIdentical else 'false'},
        { 'name': 'test_suite_id', 'type': 'string', 'value': self._testSuiteId},
        { 'name': 'execution_id', 'type': 'string', 'value':  self._executionId},
        { 'name': 'alert_id', 'type': 'string', 'value':  alertId},
        ]

        testHistoryQuery = getInsertQuery(
            valueSets, CitoTableType.TestHistoryNominal)
        historyEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testHistoryQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not historyEntryInsertResult.success:
            raise Exception(historyEntryInsertResult.error)

    def _insertHistoryEntry(self, value: str, isAnomaly: bool, alertId: Union[str, None]):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'value', 'type': 'float', 'value': value},
            {'name': 'is_anomaly', 'type': 'boolean',
                'value': 'true' if isAnomaly else 'false'},
            {'name': 'user_feedback_is_anomaly', 'type': 'integer', 'value': -1},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
            {'name': 'alert_id', 'type': 'string', 'value': alertId},
        ]

        testHistoryQuery = getInsertQuery(
            valueSets, CitoTableType.TestHistory)
        historyEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testHistoryQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not historyEntryInsertResult.success:
            raise Exception(historyEntryInsertResult.error)

    def _insertNominalTestResultEntry(self, testResult: NominalResultDto):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'expected_value', 'type': 'string',
             'value': json.dumps(testResult.expectedValue) if testResult.expectedValue else None},
            {'name': 'deviation', 'type': 'string', 'value': testResult.deviations},
            {'name': 'is_identical', 'type': 'boolean',
                'value': testResult.isIdentical},
            {'name': 'test_suite_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
        ]

        testResultQuery = getInsertQuery(
            valueSets, CitoTableType.TestResultsNominal)
        resultEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testResultQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not resultEntryInsertResult.success:
            raise Exception(resultEntryInsertResult.error)



    def _insertResultEntry(self, testResult: AnomalyTestResultDto):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'mean_ad', 'type': 'float',
             'value': testResult.meanAbsoluteDeviation},
            {'name': 'median_ad', 'type': 'float',
             'value': testResult.medianAbsoluteDeviation},
            {'name': 'modified_z_score', 'type': 'float',
             'value': testResult.modifiedZScore},
            {'name': 'expected_value', 'type': 'float',
             'value': testResult.expectedValue},
            {'name': 'expected_value_upper_bound', 'type': 'float',
             'value': testResult.expectedValueUpperBound},
            {'name': 'expected_value_lower_bound', 'type': 'float',
             'value': testResult.expectedValueLowerBound},
            {'name': 'deviation', 'type': 'float', 'value': testResult.deviation},
            {'name': 'is_anomalous', 'type': 'boolean',
                'value': testResult.isAnomaly},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
        ]

        testResultQuery = getInsertQuery(
            valueSets, CitoTableType.TestResults)
        resultEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testResultQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not resultEntryInsertResult.success:
            raise Exception(resultEntryInsertResult.error)

    def _insertAlertEntry(self, id, message: str, tableType: CitoTableType):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': id},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'message', 'type': 'string', 'value': message},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
        ]

        testAlertQuery = getInsertQuery(
            valueSets, tableType)
        alertEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testAlertQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not alertEntryInsertResult.success:
            raise Exception(alertEntryInsertResult.error)

    def _getTestEntry(self) -> QuerySnowflakeResponseDto:
        query = getTestQuery(self._testSuiteId, self._testType)

        return self._querySnowflake.execute(QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

    def _getHistoricalData(self) -> QuerySnowflakeResponseDto:
        query = getHistoryQuery(self._testSuiteId)
        getHistoricalDataResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not getHistoricalDataResult.success:
            raise Exception(getHistoricalDataResult.error)
        if not getHistoricalDataResult.value:
            raise Exception(
                'Sf query error - operation: history data')

        return [element['VALUE']
                for element in getHistoricalDataResult.value.content[self._organizationId]]

    def _getLastMatSchema(self) -> Union[MaterializationSchema, None]:
        query = getLastMatSchemaQuery(self._testSuiteId)
        queryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not queryResult.success:
            raise Exception(queryResult.error)
        if not queryResult.value:
            raise Exception(
                'Sf query error - operation: last mat schema')

        return (json.loads(queryResult.value.content[self._organizationId][0]['VALUE']) if len(queryResult.value.content[self._organizationId]) else None)

    def _getNewData(self, query):
        getNewDataResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not getNewDataResult.success:
            raise Exception(getNewDataResult.error)
        if not getNewDataResult.value:
            raise Exception('Sf query error - operation: new data')

        newData = getNewDataResult.value.content[self._organizationId]
        
        return newData

    def _runModel(self, threshold: integer, newData: float, historicalData: "list[float]") -> AnomalyTestResultDto:
        return CommonModel(newData, historicalData, threshold).run()

    def _runTest(self, newDataPoint, historicalData: "list[float]") -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']
        columnName = self._testDefinition['COLUMN_NAME']
        testSuiteId = self._testDefinition['ID']
        threshold = self._testDefinition['THRESHOLD']
        executionFrequency = self._testDefinition['EXECUTION_FREQUENCY']
        targetResourceId = self._testDefinition['TARGET_RESOURCE_ID']
        

        executedOn = datetime.datetime.utcnow().isoformat()

        self._insertExecutionEntry(
            executedOn, CitoTableType.TestExecutions)

        if(len(historicalData) <= self._MIN_HISTORICAL_DATA_NUMBER_TEST_CONDITION):
            self._insertHistoryEntry(
                newDataPoint, False, None)

            return AnomalyTestExecutionResult(testSuiteId, self._testDefinition['TEST_TYPE'], self._executionId, targetResourceId, self._organizationId, True, None, None)

        testResult = self._runModel(
            threshold, newDataPoint, historicalData)

        self._insertResultEntry(testResult)

        anomalyMessage = getAnomalyMessage(targetResourceId, databaseName, schemaName, materializationName, columnName, self._testDefinition['TEST_TYPE'])

        alertData = None
        alertId = None
        if testResult.isAnomaly:
            alertId = str(uuid.uuid4())
            self._insertAlertEntry(
                alertId, anomalyMessage, CitoTableType.TestAlerts)

            alertData = AnomalyTestAlertData(alertId, anomalyMessage, databaseName, schemaName, materializationName, materializationType, testResult.expectedValueUpperBound,
                                                  testResult.expectedValueLowerBound, columnName, newDataPoint)

        testData = AnomalyTestData(executedOn, testResult.isAnomaly, testResult.modifiedZScore, testResult.deviation)
        
        self._insertHistoryEntry(
            newDataPoint, testResult.isAnomaly, alertId)

        return AnomalyTestExecutionResult(testSuiteId, self._testDefinition['TEST_TYPE'], self._executionId, targetResourceId, self._organizationId, False, testData, alertData)

    def _runSchemaChangeModel(self, oldSchema: MaterializationSchema, newSchema: MaterializationSchema) -> NominalResultDto:
        return SchemaChangeModel(newSchema, oldSchema).run()

    def _runSchemaChangeTest(self, oldSchema: MaterializationSchema, newSchema: MaterializationSchema) -> NominalTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']
        columnName = self._testDefinition['COLUMN_NAME']
        testSuiteId = self._testDefinition['ID']
        testType = self._testDefinition['TEST_TYPE']
        targetResourceId = self._testDefinition['TARGET_RESOURCE_ID']
        
        executedOn = datetime.datetime.utcnow().isoformat()

        testResult = self._runSchemaChangeModel(
            oldSchema, newSchema)

        self._insertExecutionEntry(
            executedOn, CitoTableType.TestExecutionsNominal)

        self._insertNominalTestResultEntry(testResult)

        anomalyMessage = getAnomalyMessage(targetResourceId, databaseName, schemaName, materializationName, columnName, self._testDefinition['TEST_TYPE'])

        alertData = None
        alertId = None
        if not testResult.isIdentical:
            alertId = str(uuid.uuid4())
            self._insertAlertEntry(
                alertId, anomalyMessage, CitoTableType.TestAlertsNominal)

            alertData = NominalTestAlertData(alertId, anomalyMessage, databaseName, schemaName, materializationName, materializationType, testResult.deviations)

        self._insertNominalHistoryEntry(
            newSchema, testResult.isIdentical, alertId)

        testData = NominalTestData(executedOn, testResult.deviations, testResult.isIdentical)
        return NominalTestExecutionResult(testSuiteId, testType, self._executionId, targetResourceId, self._organizationId, testData, alertData)
        
    def _runMaterializationRowCountTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']

        newDataQuery = getRowCountQuery(
            databaseName, schemaName, materializationName, MaterializationType[materializationType])

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                f'Mat row count - More than one or no matching new data entries found')

        newDataPoint = newData[0]['ROW_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationColumnCountTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']

        newDataQuery = getColumnCountQuery(
            databaseName, schemaName, materializationName)

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                'Mat column count - More than one or no matching new data entries found')

        newDataPoint = newData[0]['COLUMN_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationFreshnessTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']

        newDataQuery = getFreshnessQuery(
            databaseName, schemaName, materializationName, materializationType)

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                'Mat freshness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationSchemaChangeTest(self) -> NominalTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']

        newDataQuery = getSchemaChangeQuery(
            databaseName, schemaName, materializationName)

        newData = self._getNewData(newDataQuery)

        newSchema = {}
        for el in newData:
            columnDefinition = el['COLUMN_DEFINITION']
            ordinalPosition = columnDefinition['ORDINAL_POSITION']
            newSchema[str(ordinalPosition)] = columnDefinition

        oldSchema = self._getLastMatSchema()

        testResult = self._runSchemaChangeTest(oldSchema, newSchema)

        return testResult

    def _runColumnCardinalityTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getCardinalityQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                'Col cardinality - More than one or no matching new data entries found')

        newDataPoint = newData[0]['DISTINCT_VALUE_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnDistributionTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getDistributionQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                'Col Distribution - More than one or no matching new data entries found')

        newDataPoint = newData[0]['MEDIAN']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnFreshnessTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getColumnFreshnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                'Col Freshness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnNullnessTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getNullnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                'Col Nullness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['NULLNESS_RATE']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnUniquenessTest(self) -> AnomalyTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getUniquenessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if(len(newData) != 1):
            raise Exception(
                'Col Uniqueness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['UNIQUENESS_RATE']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _getTestDefinition(self):
        getTestEntryResult = self._getTestEntry()

        if not getTestEntryResult.success:
            raise Exception(getTestEntryResult.error)
        if not getTestEntryResult.value:
            raise Exception(f'Sf query error - operation: test entry')

        organizationResult = getTestEntryResult.value.content[self._organizationId]
        if not len(organizationResult) == 1:
            raise Exception('Test Definition - More than one or no test found')

        return organizationResult[0]

    def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
        try:
            if auth.isSystemInternal and not request.targetOrgId:
                raise Exception('Target organization id missing');
            if not auth.isSystemInternal and not auth.callerOrgId:
                raise Exception('Caller organization id missing');
            if not request.targetOrgId and not auth.callerOrgId:
                raise Exception('No organization Id instance provided');
            if request.targetOrgId and auth.callerOrgId:
                raise Exception('callerOrgId and targetOrgId provided. Not allowed');

            self._testSuiteId = request.testSuiteId
            self._testType = request.testType
            self._targetOrgId = request.targetOrgId
            self._organizationId = request.targetOrgId if request.targetOrgId else auth.callerOrgId
            self._requestLoggingInfo = f'(organizationId: {self._organizationId}, testSuiteId: {self._testSuiteId}, testType: {self._testType})'
            self._executionId = str(uuid.uuid4())
            self._jwt = auth.jwt
            self._testDefinition = self._getTestDefinition()

            testTypeKey = 'TEST_TYPE'

            if self._testDefinition[testTypeKey] == AnomalyMatTest.MaterializationRowCount.value:
                testResult = self._runMaterializationRowCountTest()
            elif self._testDefinition[testTypeKey] == AnomalyMatTest.MaterializationColumnCount.value:
                testResult = self._runMaterializationColumnCountTest()
            elif self._testDefinition[testTypeKey] == AnomalyMatTest.MaterializationFreshness.value:
                testResult = self._runMaterializationFreshnessTest()
            elif self._testDefinition[testTypeKey] == AnomalyColumnTest.ColumnCardinality.value:
                testResult = self._runColumnCardinalityTest()
            elif self._testDefinition[testTypeKey] == AnomalyColumnTest.ColumnDistribution.value:
                testResult = self._runColumnDistributionTest()
            elif self._testDefinition[testTypeKey] == AnomalyColumnTest.ColumnFreshness.value:
                testResult = self._runColumnFreshnessTest()
            elif self._testDefinition[testTypeKey] == AnomalyColumnTest.ColumnNullness.value:
                testResult = self._runColumnNullnessTest()
            elif self._testDefinition[testTypeKey] == AnomalyColumnTest.ColumnUniqueness.value:
                testResult = self._runColumnUniquenessTest()
            elif self._testDefinition[testTypeKey] == NominalMatTest.MaterializationSchemaChange.value:
                testResult = self._runMaterializationSchemaChangeTest()
            else:
                raise Exception('Test type mismatch')

            return Result.ok(testResult)

        except Exception as e:
            logger.exception(f'error: {e}' if e.args[0] else f'error: unknown - {self._requestLoggingInfo}')
            return Result.fail('')
