from dataclasses import dataclass
from typing import Union


@dataclass
class _TestData:
    executedOn: str


@dataclass
class QuantTestData(_TestData):
    isAnomolous: bool
    modifiedZScore: float
    deviation: float


@dataclass
class QualTestData(_TestData):
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
class QuantTestAlertData(_AlertData):
    expectedUpper: Union[float, None]
    expectedLower: Union[float, None]
    columnName: Union[str, None]
    value: float


@dataclass
class QualTestAlertData(_AlertData):
    deviatons: str


@dataclass
class _TestExecutionResult:
    testSuiteId: str
    testType: str
    executionId: str
    targetResourceId: str
    organizationId: str


@dataclass
class QuantTestExecutionResult(_TestExecutionResult):
    isWarmup: bool
    testData: Union[QuantTestData, None]
    alertData: Union[QuantTestAlertData, None]


@dataclass
class QualTestExecutionResult(_TestExecutionResult):
    testData: QualTestData
    alertData: Union[QualTestAlertData, None]
