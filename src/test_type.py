from enum import Enum


class QualMatTest(Enum):
    MaterializationSchemaChange = 'MaterializationSchemaChange'


class QuantMatTest (Enum):
    MaterializationRowCount = 'MaterializationRowCount'
    MaterializationColumnCount = 'MaterializationColumnCount'
    MaterializationFreshness = 'MaterializationFreshness'


class QuantColumnTest(Enum):
    ColumnFreshness = 'ColumnFreshness'
    ColumnCardinality = 'ColumnCardinality'
    ColumnUniqueness = 'ColumnUniqueness'
    ColumnNullness = 'ColumnNullness'
    ColumnDistribution = 'ColumnDistribution'

class CustomTest(Enum):
    CustomTest = 'Custom'