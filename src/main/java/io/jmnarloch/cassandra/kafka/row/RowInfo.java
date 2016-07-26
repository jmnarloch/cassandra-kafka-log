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
package io.jmnarloch.cassandra.kafka.row;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class RowInfo implements Iterable<CellInfo> {

    private final CFMetaData metaData;

    private final DecoratedKey partitionKey;

    private final Row rawRow;

    private final Collection<CellInfo> cells;

    public RowInfo(CFMetaData metadata, DecoratedKey partitionKey, Row rawRow) {
        this.metaData = metadata;
        this.partitionKey = partitionKey;
        this.rawRow = rawRow;
        this.cells = materializeCells();
    }

    private Collection<CellInfo> materializeCells() {
        final Collection<CellInfo> cells = new ArrayList<>();
        addPartitionColumns(cells);
        addClusteringColumns(cells);
        addColumns(cells);
        return cells;
    }

    private void addPartitionColumns(Collection<CellInfo> cells) {
        final PartitionColumns partitionColumns = metaData.partitionColumns();
        for (ColumnDefinition columnDefinition : partitionColumns) {
            cells.add(new CellInfo(columnDefinition, partitionKey.getKey()));
        }
    }

    private void addClusteringColumns(Collection<CellInfo> cells) {
        final List<ColumnDefinition> columnDefinitions = metaData.clusteringColumns();
        final Clustering clusteringColumns = rawRow.clustering();
        for (int index = 0; index < metaData.clusteringColumns().size(); index++) {
            cells.add(new CellInfo(columnDefinitions.get(index), clusteringColumns.get(index)));
        }
    }

    private void addColumns(Collection<CellInfo> cells) {
        for (ColumnData columnData : rawRow) {
            if (columnData instanceof Cell) {
                cells.add(new CellInfo(columnData.column(), ((Cell) columnData).value()));
            }
        }
    }

    @Override
    public Iterator<CellInfo> iterator() {

        return null;
    }
}
