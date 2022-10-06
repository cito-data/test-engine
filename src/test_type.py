from enum import Enum

class NominalMatTest(Enum):
    MaterializationSchemaChange = 'MaterializationSchemaChange'

class AnomalyMatTest (Enum):
    MaterializationRowCount = 'MaterializationRowCount'
    MaterializationColumnCount = 'MaterializationColumnCount'
    MaterializationFreshness = 'MaterializationFreshness'

class AnomalyColumnTest(Enum):
    ColumnFreshness = 'ColumnFreshness'
    ColumnCardinality = 'ColumnCardinality'
    ColumnUniqueness = 'ColumnUniqueness'
    ColumnNullness = 'ColumnNullness'
    ColumnDistribution = 'ColumnDistribution'
