package io.airbyte.integrations.destination.s3_data_lake

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.ImportType
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.data.*
import io.airbyte.cdk.load.data.iceberg.parquet.toIcebergRecord
import io.airbyte.cdk.load.message.EnrichedDestinationRecordAirbyteValue
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergUtil
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.Operation
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.RecordWrapper
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.AIRBYTE_CDC_DELETE_COLUMN
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.transformValueRecursingIntoArrays
import io.airbyte.cdk.load.util.serializeToString
import org.apache.iceberg.Schema
import org.apache.iceberg.data.Record
import java.math.BigDecimal
import java.math.BigInteger

/**
 * Extension utility that mirrors [IcebergUtil.toRecord] but stringifies the
 * `_airbyte_meta` column before building the Iceberg [Record].
 */
fun IcebergUtil.toRecordWithMetaAsString(
    record: EnrichedDestinationRecordAirbyteValue,
    stream: DestinationStream,
    tableSchema: Schema,
): Record {
    record.declaredFields.forEach { (_, value) ->
        value.transformValueRecursingIntoArrays { element, elementType ->
            when (elementType) {
                ArrayTypeWithoutSchema,
                is ObjectType,
                ObjectTypeWithEmptySchema,
                ObjectTypeWithoutSchema,
                is UnionType,
                is UnknownType ->
                    ChangedValue(StringValue(element.serializeToString()), null)
                is NumberType -> nullOutOfRangeNumber(element)
                is IntegerType -> nullOutOfRangeInt(element)
                else -> null
            }
        }
    }

    val allFields = record.allTypedFields.toMutableMap()
    val metaField = allFields[Meta.COLUMN_NAME_AB_META]
    if (metaField != null) {
        allFields[Meta.COLUMN_NAME_AB_META] =
            EnrichedAirbyteValue(
                StringValue(metaField.abValue.serializeToString()),
                metaField.type,
                name = metaField.name,
                airbyteMetaField = metaField.airbyteMetaField,
                changes = metaField.changes,
            )
    }

    return RecordWrapper(
        delegate = allFields.toMap().toIcebergRecord(tableSchema),
        operation = getOperation(record, stream.importType),
    )
}

private fun getOperation(record: EnrichedDestinationRecordAirbyteValue, importType: ImportType) =
    if (record.declaredFields[AIRBYTE_CDC_DELETE_COLUMN] != null &&
        record.declaredFields[AIRBYTE_CDC_DELETE_COLUMN]!!.abValue !is NullValue) {
        Operation.DELETE
    } else if (importType is Dedupe) {
        Operation.UPDATE
    } else {
        Operation.INSERT
    }

private fun nullOutOfRangeInt(numberValue: AirbyteValue): ChangedValue? =
    if (BigInteger.valueOf(Long.MIN_VALUE) <= (numberValue as IntegerValue).value &&
        numberValue.value <= BigInteger.valueOf(Long.MAX_VALUE)) {
        null
    } else {
        ChangedValue(
            NullValue,
            ChangeDescription(Change.NULLED, Reason.DESTINATION_FIELD_SIZE_LIMITATION),
        )
    }

private fun nullOutOfRangeNumber(numberValue: AirbyteValue): ChangedValue? =
    if (BigDecimal(-Double.MAX_VALUE) <= (numberValue as NumberValue).value &&
        numberValue.value <= BigDecimal(Double.MAX_VALUE)) {
        null
    } else {
        ChangedValue(
            NullValue,
            ChangeDescription(Change.NULLED, Reason.DESTINATION_FIELD_SIZE_LIMITATION),
        )
    }
