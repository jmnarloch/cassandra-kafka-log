package io.jmnarloch.cassandra.kafka.formatter;

import io.jmnarloch.cassandra.kafka.exception.FormatterException;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

public class JacksonFormatter implements Formatter {

    private final ObjectMapper objectMapper;

    public JacksonFormatter() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] format(Row row) {

        try {
            final ObjectNode document = objectMapper.createObjectNode();
            document.put("columns", formatColumns(row));
            return writeAsBytes(document);
        } catch (IOException e) {
            throw new FormatterException("Could not encode the column family", e);
        }
    }

    private byte[] writeAsBytes(ObjectNode document) throws IOException {
        return objectMapper.writer().writeValueAsBytes(document);
    }

    private ObjectNode formatColumns(Row row) throws CharacterCodingException {
        final ObjectNode columns = objectMapper.createObjectNode();
        for (ColumnDefinition definition : row.columns()) {
            final ByteBuffer data = row.getCell(definition).value();
            columns.put(definition.name.toString(), ByteBufferUtil.string(data));
        }
        return columns;
    }
}
