package com.bakdata.profilestore.core.fields;

import com.bakdata.profilestore.common.FieldType;

public abstract class FieldHandler implements IdExtractor, FieldUpdater {
    private final FieldType fieldType;

    public FieldHandler(final FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public FieldType type() {
        return this.fieldType;
    }

}
