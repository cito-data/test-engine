from enum import Enum


class TestType(Enum):
    Anomaly = 'Anomaly'
    Nominal = 'Nominal'

class AnomalyTest(Enum):
    ColumnFreshness = 'ColumnFreshness'
    ColumnCardinality = 'ColumnCardinality'
    ColumnUniqueness = 'ColumnUniqueness'
    ColumnNullness = 'ColumnNullness'
    ColumnDistribution = 'ColumnDistribution'
    MaterializationRowCount = 'MaterializationRowCount'
    MaterializationColumnCount = 'MaterializationColumnCount'
    MaterializationFreshness = 'MaterializationFreshness'

class NominalTest(Enum):
    MaterializationSchemaChange = 'MaterializationSchemaChange'