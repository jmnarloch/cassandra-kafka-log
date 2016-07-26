/**
 * Copyright (c) 2015-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jmnarloch.cassandra.kafka.formatter;

import io.jmnarloch.cassandra.kafka.exception.FormatterException;
import io.jmnarloch.cassandra.kafka.row.CellInfo;
import io.jmnarloch.cassandra.kafka.row.RowInfo;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

public class JacksonFormatter implements Formatter {

    private static final String COLUMNS = "columns";

    private final ObjectMapper objectMapper;

    public JacksonFormatter() {
        this(new ObjectMapper());
    }

    public JacksonFormatter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] format(RowInfo row) {
        try {
            final JsonNode document = objectMapper.createObjectNode()
                    .put(COLUMNS, formatColumns(row));
            return writeAsBytes(document);
        } catch (IOException e) {
            throw new FormatterException("Could not encode the row", e);
        }
    }

    private ObjectNode formatColumns(RowInfo row) throws CharacterCodingException {
        final ObjectNode columns = objectMapper.createObjectNode();
        for (CellInfo cell : row) {
            columns.put(cell.getName(), cell.getValueAsString());
        }
        return columns;
    }

    private byte[] writeAsBytes(JsonNode document) throws IOException {
        return objectMapper.writer().writeValueAsBytes(document);
    }
}
