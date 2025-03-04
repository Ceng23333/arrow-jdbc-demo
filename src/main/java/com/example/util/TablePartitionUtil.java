package com.example.util;

import org.apache.commons.lang3.StringUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TablePartitionUtil {
    public static final String LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH = ";";
    public static final String LAKESOUL_RANGE_PARTITION_SPLITTER = ",";
    public static final String LAKESOUL_HASH_PARTITION_SPLITTER = ",";

    public static TablePartitionKeys parseTableInfoPartitions(String partitions) {
        if (StringUtils.isBlank(partitions) || partitions.equals(LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH)) {
            return new TablePartitionKeys();
        }
        String[] rangeAndPks = StringUtils.split(partitions, LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH);
        
        // 有哈希键，无范围键
        if (StringUtils.startsWith(partitions, LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH)) {
            return new TablePartitionKeys(
                    Arrays.asList(StringUtils.split(rangeAndPks[0], LAKESOUL_HASH_PARTITION_SPLITTER)),
                    Collections.emptyList());
        }
        
        // 有范围键，无主键
        if (StringUtils.endsWith(partitions, LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH)) {
            return new TablePartitionKeys(
                    Collections.emptyList(),
                    Arrays.asList(StringUtils.split(rangeAndPks[0], LAKESOUL_RANGE_PARTITION_SPLITTER)));
        }
        
        // 同时具有范围键和主键
        return new TablePartitionKeys(
                Arrays.asList(StringUtils.split(rangeAndPks[1], LAKESOUL_HASH_PARTITION_SPLITTER)),
                Arrays.asList(StringUtils.split(rangeAndPks[0], LAKESOUL_RANGE_PARTITION_SPLITTER))
        );
    }

    public static class TablePartitionKeys {
        private final List<String> primaryKeys;
        private final List<String> rangeKeys;

        public TablePartitionKeys() {
            this.primaryKeys = Collections.emptyList();
            this.rangeKeys = Collections.emptyList();
        }

        public TablePartitionKeys(List<String> pks, List<String> rks) {
            this.primaryKeys = pks;
            this.rangeKeys = rks;
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }

        public List<String> getRangeKeys() {
            return rangeKeys;
        }

        public String getPKString() {
            if (primaryKeys.isEmpty()) return "";
            return String.join(LAKESOUL_HASH_PARTITION_SPLITTER, primaryKeys);
        }

        public String getRangeKeyString() {
            if (rangeKeys.isEmpty()) return "";
            return String.join(LAKESOUL_RANGE_PARTITION_SPLITTER, rangeKeys);
        }
    }
} 