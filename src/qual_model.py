
from dataclasses import dataclass
from typing import Union


@dataclass
class ColumnDefinition:
    columnName: str
    dataType: str
    isIdentity: bool
    isNullable: bool
    ordinalPosition: int


@dataclass
class SchemaDiff:
    column_name: "tuple[Union[str, None], Union[str, None]]"
    ordinal_position: "tuple[Union[int, None], Union[int, None]]"
    data_type: Union["tuple[Union[str, None], Union[str, None]]", None]
    is_identity: Union["tuple[Union[bool, None], Union[bool, None]]", None]
    is_nullable: Union["tuple[Union[bool, None], Union[bool, None]]", None]


@dataclass
class ResultDto:
    isIdentical: bool
    expectedValue: Union["dict[str, ColumnDefinition]", None]
    value: "dict[str, ColumnDefinition]"
    deviations: "list[SchemaDiff]"


class SchemaChangeModel():

    _oldSchema: Union["dict[str, ColumnDefinition]", None]
    _newSchema: "dict[str, ColumnDefinition]"

    _schemaDiffs: "list[SchemaDiff]"

    def __init__(self, newSchema: "dict[str,ColumnDefinition]", oldSchema: Union["dict[str, ColumnDefinition]", None]) -> None:
        self._oldSchema = oldSchema
        self._newSchema = newSchema

    def run(self) -> ResultDto:
        if not self._oldSchema:
            return ResultDto(True, self._oldSchema, self._newSchema, [])

        columnCountOldSchema = len(self._oldSchema)
        columnCountNewSchema = len(self._newSchema)

        schemaDiffs: list[SchemaDiff] = []
        for i in range(1, (columnCountNewSchema if columnCountNewSchema > columnCountOldSchema else columnCountOldSchema) + 1):
            oldColumnSchemaDefinition = self._oldSchema[str(
                i)] if i <= columnCountOldSchema else None
            newColumnSchemaDefinition = self._newSchema[str(
                i)] if i <= columnCountNewSchema else None

            if (oldColumnSchemaDefinition and not newColumnSchemaDefinition):
                schemaDiff = SchemaDiff((oldColumnSchemaDefinition.columnName, None),
                                        (oldColumnSchemaDefinition.ordinalPosition, None),
                                        (oldColumnSchemaDefinition.dataType, None),
                                        (oldColumnSchemaDefinition.isIdentity, None),
                                        (oldColumnSchemaDefinition.isNullable, None))

                schemaDiffs.append(schemaDiff)
            elif (newColumnSchemaDefinition and not oldColumnSchemaDefinition):

                schemaDiff = SchemaDiff((None, newColumnSchemaDefinition.columnName),
                                        (None,
                                         newColumnSchemaDefinition.ordinalPosition),
                                        (None,
                                         newColumnSchemaDefinition.dataType),
                                        (None,
                                         newColumnSchemaDefinition.isIdentity),
                                        (None, newColumnSchemaDefinition.isNullable))

                schemaDiffs.append(schemaDiff)
            else:
                oldColumnName = oldColumnSchemaDefinition.columnName if oldColumnSchemaDefinition else None
                newColumnName = newColumnSchemaDefinition.columnName if newColumnSchemaDefinition else None
                columnNameIdentical = oldColumnName == newColumnName

                oldDataType = oldColumnSchemaDefinition.dataType if oldColumnSchemaDefinition else None
                newDataType = newColumnSchemaDefinition.dataType if newColumnSchemaDefinition else None
                dataTypeIdentical = oldDataType == newDataType

                oldOrdinalPosition = oldColumnSchemaDefinition.ordinalPosition if oldColumnSchemaDefinition else None
                newOrdinalPosition = newColumnSchemaDefinition.ordinalPosition if newColumnSchemaDefinition else None
                ordinalPositionIdentical = oldOrdinalPosition == newOrdinalPosition

                oldIsIdentity = oldColumnSchemaDefinition.isIdentity if oldColumnSchemaDefinition else None
                newIsIdentity = newColumnSchemaDefinition.isIdentity if newColumnSchemaDefinition else None
                isIdentityIdentical = oldIsIdentity == newIsIdentity

                oldIsNullable = oldColumnSchemaDefinition.isNullable if oldColumnSchemaDefinition else None
                newIsNullable = newColumnSchemaDefinition.isNullable if newColumnSchemaDefinition else None
                isNullableIdentical = oldIsNullable == newIsNullable

                anyDiff = not (
                    columnNameIdentical and ordinalPositionIdentical and dataTypeIdentical and isIdentityIdentical and isNullableIdentical)

                if anyDiff:

                    schemaDiff = SchemaDiff((oldColumnName, newColumnName),
                                            (oldOrdinalPosition,
                                             newOrdinalPosition),
                                            (oldDataType, newDataType) if not dataTypeIdentical else None,
                                            (oldIsIdentity,
                                             newIsIdentity) if not isIdentityIdentical else None,
                                            (oldIsNullable,
                                             newIsNullable) if not isNullableIdentical else None
                                            )

                    schemaDiffs.append(schemaDiff)

        return ResultDto(not len(schemaDiffs), self._oldSchema, self._newSchema, schemaDiffs)
