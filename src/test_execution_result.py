from dataclasses import dataclass
from typing import Union

from qual_model import SchemaDiff


@dataclass
class AnomalyData:
    importance: float


@dataclass
class _TestData:
    executedOn: str


@dataclass
class QuantTestData(_TestData):
    detectedValue: float
    expectedUpperBound: float
    expectedLowerBound: float
    modifiedZScore: Union[float, None]
    deviation: float
    anomaly: Union[AnomalyData, None]


@dataclass
class QualTestData(_TestData):
    deviations: "list[SchemaDiff]"
    isIdentical: bool

@dataclass
class CustomTestData(_TestData):
    detectedValue: float
    expectedUpperBound: float
    expectedLowerBound: float
    modifiedZScore: Union[float, None]
    deviation: float
    anomaly: Union[AnomalyData, None]

@dataclass
class _AlertData:
    alertId: str
    message: str

@dataclass
class QuantTestAlertData(_AlertData):
    databaseName: str
    schemaName: str
    materializationName: str
    materializationType: str
    expectedValue: float
    columnName: str


@dataclass
class QualTestAlertData(_AlertData):
    databaseName: str
    schemaName: str
    materializationName: str
    materializationType: str
    deviatons: "list[SchemaDiff]"

@dataclass
class CustomTestAlertData(_AlertData):
    expectedValue: float

@dataclass
class _TestExecutionResult:
    testSuiteId: str
    testType: str
    executionId: str
    organizationId: str


@dataclass
class QuantTestExecutionResult(_TestExecutionResult):
    targetResourceId: str
    isWarmup: bool
    testData: Union[QuantTestData, None]
    alertData: Union[QuantTestAlertData, None]
    lastAlertSent: str


@dataclass
class QualTestExecutionResult(_TestExecutionResult):
    targetResourceId: str
    testData: QualTestData
    alertData: Union[QualTestAlertData, None]
    lastAlertSent: str

@dataclass
class CustomTestExecutionResult(_TestExecutionResult):
    targetResourceIds: "list[str]"
    isWarmup: bool
    testData: Union[CustomTestData, None]
    alertData: Union[CustomTestAlertData, None]
    lastAlertSent: str