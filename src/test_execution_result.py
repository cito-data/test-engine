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
class _AlertData:
    alertId: str
    message: str
    databaseName: str
    schemaName: str
    materializationName: str
    materializationType: str


@dataclass
class QuantTestAlertData(_AlertData):
    expectedValue: float
    columnName: str


@dataclass
class QualTestAlertData(_AlertData):
    deviatons: "list[SchemaDiff]"


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
    lastAlertSent: str


@dataclass
class QualTestExecutionResult(_TestExecutionResult):
    testData: QualTestData
    alertData: Union[QualTestAlertData, None]
    lastAlertSent: str
