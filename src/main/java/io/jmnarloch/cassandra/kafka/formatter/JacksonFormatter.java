/**
 * Copyright (c) 2015-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
