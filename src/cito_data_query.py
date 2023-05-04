from enum import Enum
from typing import Any, Union
from pymongo import database
from test_type import QuantColumnTest, QuantMatTest, QualMatTest, CustomTest


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
    TestSuitesCustom = 'test_suites_custom'


quantColumnTest = set(item.value for item in QuantColumnTest)
quantMatTest = set(item.value for item in QuantMatTest)
qualMatTest = set(item.value for item in QualMatTest)
customTest = set(item.value for item in CustomTest)


def insertTableData(document: "dict[str, Any]", tableType: CitoTableType, dbConnection: database.Database, organizationId: str):
    collection = dbConnection[tableType.value + '_' + organizationId]

    result = collection.insert_one(document)

    if not result.acknowledged:
        raise Exception('Insertion of documents failed')

def getHistoryData(testSuiteId: str, dbConnection: database.Database, organizationId: str):
    collection = dbConnection[CitoTableType.TestHistory.value + '_' + organizationId]
    pipeline = [
        {
          '$match': {
            'test_suite_id': testSuiteId,
            '$or': [
              { 'is_anomaly': { '$ne': True } },
              { 'user_feedback_is_anomaly': { '$eq': 0 } }
            ]
          }
        },
        {
          '$lookup': {
            'from': 'test_executions_' + organizationId,
            'localField': 'execution_id',
            'foreignField': 'id',
            'as': 'test_executions'
          }
         },
         {
          '$unwind': {
            'path': '$test_executions',
            'preserveNullAndEmptyArrays': False
          }
         },
         {
          '$project': {
            '_id': 0,
            'executed_on': '$test_executions.executed_on',
            'value': 1
          }
         }
    ]

    results = list(collection.aggregate(pipeline))

    if results is not None:
        return results
    else:
        raise Exception('History data matching testSuiteId not found')

def getLastMatSchemaData(testSuiteId: str, dbConnection: database.Database, organizationId: str):
    testExecQualCollectionName = CitoTableType.TestExecutionsQual.value + '_' + organizationId

    testExecQualCollection = dbConnection[testExecQualCollectionName]
    # executionIdCte = list(testExecQualCollection.find({ 'test_suite_id': testSuiteId }).sort('executed_on', -1).limit(1))
    
    testHistoryQualCollectionName = CitoTableType.TestHistoryQual.value + '_' + organizationId

    pipeline = [
        {
            '$match': {
                'test_suite_id': testSuiteId
            }
        },
        {
            '$sort': {
                'executed_on': -1
            }
        },
        {
            '$limit': 1
        },
        {
            '$lookup': {
                'from': testHistoryQualCollectionName,
                'localField': 'id',
                'foreignField': 'execution_id',
                'as': 'test_history_qual' 
            }
        },
        {
            '$unwind': {
                'path': '$test_history_qual',
                'preserveNullAndEmptyArrays': False
            }
        },
        {
            '$project': {
                'id': 1,
                'value': '$test_history_qual.value'
            }
        }
    ]

    results = list(testExecQualCollection.aggregate(pipeline))

    return results

def getTestData(testSuiteId: str, testType: Union[QuantColumnTest, QuantMatTest, QualMatTest, CustomTest], dbConnection: database.Database, organizationId: str):

    if testType in quantColumnTest or testType in quantMatTest:
        table = CitoTableType.TestSuites
    elif testType in customTest:
        table = CitoTableType.TestSuitesCustom
    else:
        table = CitoTableType.TestSuitesQual

    collection = dbConnection[table.value + '_' + organizationId]

    result = collection.find_one({ 'id': testSuiteId })

    if result is not None:
        return result
    else:
        raise Exception('Test data matching testSuiteId not found')

def updateTableData(testSuiteId: str, tableType: CitoTableType, columnName: str, value: str, dbConnection: database.Database, organizationId: str):
    collection = dbConnection[tableType.value + '_' + organizationId]

    result = collection.update_one({ 'id': testSuiteId }, { '$set': { columnName: value } })

    if result.modified_count != 1:
        raise Exception('Updating document failed')