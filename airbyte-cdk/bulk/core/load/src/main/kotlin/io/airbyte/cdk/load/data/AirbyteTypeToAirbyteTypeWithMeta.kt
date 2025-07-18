/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.data

import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.message.Meta.AirbyteMetaFields

class AirbyteTypeToAirbyteTypeWithMeta(
    private val flatten: Boolean,
    private val metaStorage: MetaStorage,
    private val metaNullable: Boolean,
) {
    fun convert(schema: AirbyteType): ObjectType {
        val properties =
            linkedMapOf(
                Meta.COLUMN_NAME_AB_RAW_ID to
                    FieldType(AirbyteMetaFields.RAW_ID.type, nullable = false),
                Meta.COLUMN_NAME_AB_EXTRACTED_AT to
                    FieldType(AirbyteMetaFields.EXTRACTED_AT.type, nullable = false),
                Meta.COLUMN_NAME_AB_META to
                    FieldType(
                        if (metaStorage == MetaStorage.STRING) StringType else AirbyteMetaFields.META.type,
                        nullable = metaNullable,
                    ),
                Meta.COLUMN_NAME_AB_GENERATION_ID to
                    FieldType(AirbyteMetaFields.GENERATION_ID.type, nullable = false),
            )
        if (flatten) {
            if (schema is ObjectType) {
                schema.properties.forEach { (name, field) -> properties[name] = field }
            } else if (schema is ObjectTypeWithEmptySchema) {
                // Do nothing: no fields to add
            } else {
                throw IllegalStateException(
                    "Cannot flatten without an object schema (schema type: $schema)"
                )
            }
        } else {
            properties[Meta.COLUMN_NAME_DATA] = FieldType(schema, nullable = false)
        }
        return ObjectType(properties)
    }
}

fun AirbyteType.withAirbyteMeta(
    flatten: Boolean = false,
    metaStorage: MetaStorage = MetaStorage.OBJECT,
    metaNullable: Boolean = false,
): ObjectType =
    AirbyteTypeToAirbyteTypeWithMeta(flatten, metaStorage, metaNullable).convert(this)
