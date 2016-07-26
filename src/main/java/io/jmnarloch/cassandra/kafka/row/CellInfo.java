package io.jmnarloch.cassandra.kafka.row;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

public final class CellInfo {

    private final ColumnDefinition columnDefinition;

    private final ByteBuffer value;

    public CellInfo(ColumnDefinition columnDefinition, ByteBuffer value) {
        this.columnDefinition = columnDefinition;
        this.value = value;
    }

    public String getName() {
        return columnDefinition.name.toString();
    }

    public Object getValue() {
        return getType().compose(getByteValue());
    }

    public String getValueAsString() {
        return getType().getString(getByteValue());
    }

    private ByteBuffer getByteValue() {
        return value.duplicate();
    }

    private AbstractType<?> getType() {
        return columnDefinition.cellValueType();
    }
}
