package com.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.arrow.flight.auth2.Auth2Constants.AUTHORIZATION_HEADER;
import static org.apache.arrow.flight.auth2.Auth2Constants.BEARER_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;

import com.example.util.TablePartitionUtil;
import com.example.util.TablePartitionUtil.TablePartitionKeys;

public class CDCRegressionTest {

    // 连接参数
    public static final String HOST = "localhost";
    public static final int PORT = 50051;
//    public static final String HOST = "dapm-api.dmetasoul.com";
//    public static final int PORT = 443;

    
    // 数据库相关参数
    public static final String CATALOG = "lakesoul";
    public static final String SCHEMA = "default";
    public static final String TABLE_NAME = "lakesoul_test_table";
    
    // 批处理相关参数
    public static final int BATCH_SIZE = 8;
    public static final int BATCH_COUNT = 8;
    public static final int BATCH_STEP = BATCH_SIZE;
    public static final float VALUE_RATIO = 1.5f;
    public static final int MAX_STRING_LENGTH = 20;
    public static final int ORIGINAL_ROW_COUNT = 0;
    
    // 验证相关参数
    public static final double FLOAT_COMPARISON_DELTA = 0.0001;

    public static void main(String[] args) throws Exception {

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // 连接域名
        final Location clientLocation = Location.forGrpcInsecure(HOST, PORT);
//        final Location clientLocation = Location.forGrpcTls(HOST, PORT);

        // 鉴权 Token Header
        String jwtToken = "";
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(AUTHORIZATION_HEADER, BEARER_PREFIX + jwtToken);
        HeaderCallOption headerCallOption = new HeaderCallOption(headers);

//        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress("dapm.dmetasoul.com", 443);
//        channelBuilder.overrideAuthority("dapm-api.dmetasoul.com");

        // 流式写入数据
        try (FlightSqlClient flightSqlClient =
//                     new FlightSqlClient(FlightGrpcUtils.createFlightClient(allocator, channelBuilder.build()))) {
                     new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build())) {

            // 测试连接是否正常
            FlightInfo selectOneInfo = flightSqlClient.execute("SELECT 1", headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(selectOneInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("SELECT 1 result: " + root.getFieldVectors().get(0).getObject(0));
                }
            }

            // 测试连接是否正常
            FlightInfo showTables = flightSqlClient.execute("SHOW TABLES", headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(showTables.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("SHOW TABLES result:" + root.contentToTSVString());
                }
            }


            long code;
            code = flightSqlClient.executeUpdate(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME), headerCallOption);
            System.out.println("drop table return code: " + code);
            code = flightSqlClient.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS %s (name STRING, id INT PRIMARY KEY NOT NULL, score FLOAT, date DATE, region STRING) WITH (use_cdc='true') PARTITION BY (region, date) ", TABLE_NAME), headerCallOption);
            System.out.println("create table return code: " + code);
            // 测试查询表内容
            FlightInfo selectAllInfo = flightSqlClient.execute(String.format("SELECT * FROM %s", TABLE_NAME), headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(selectAllInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("SELECT * result:" + root.contentToTSVString());
                }
            }



            // 测试获取指定 catalog 下的 schema 列表
            FlightInfo schemasInfo = flightSqlClient.getSchemas(CATALOG, null, headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(schemasInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("Schema: " + root.getFieldVectors().get(1).getObject(0));
                }
            }

            // 测试获取指定 catalog 和 schema 下的表列表
            FlightInfo tablesInfo = flightSqlClient.getTables(CATALOG, SCHEMA, null, null, false, headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(tablesInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("Table: " + root.getFieldVectors().get(2).getObject(0));
                }
            }

            FlightInfo primaryKeysInfo = flightSqlClient.getPrimaryKeys(TableRef.of(CATALOG, SCHEMA, TABLE_NAME), null, headerCallOption);
            String metadata = new String(primaryKeysInfo.getAppMetadata(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> metadataMap = mapper.readValue(metadata, new TypeReference<HashMap<String, Object>>() {});

            System.out.println(metadataMap);
            TablePartitionKeys partitionKeys = TablePartitionUtil.parseTableInfoPartitions(metadataMap.get("partitions").toString());
            List<String> partitionCols = partitionKeys.getRangeKeys();
            List<String> primaryKeys = partitionKeys.getPrimaryKeys();
            String cdcColumn = metadataMap.get("lakesoul_cdc_change_column").toString();
            System.out.println(partitionCols);
            System.out.println(primaryKeys);
            
            Schema tableSchema = primaryKeysInfo.getSchemaOptional().get();
            System.out.println(tableSchema);
//            if (1==1) System.exit(0);
//            try (FlightStream stream = flightSqlClient.getStream(primaryKeysInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
//                VectorSchemaRoot root = stream.getRoot();
//                while (stream.next()) {
//                    System.out.println("Primary Key: " + root.getFieldVectors().get(0).getObject(0));
//                }
//            }

            long writeStartTime = System.currentTimeMillis();

            writeBatchStreamly(
                flightSqlClient, 
                null, 
                headerCallOption, 
                allocator, 
                tableSchema, 
                TABLE_NAME, 
                CATALOG, 
                SCHEMA, 
                BATCH_SIZE,
                BATCH_COUNT,
                0,
                partitionCols,
                primaryKeys,
                cdcColumn
            );
            printTimeElapsed("无事务数据写入", writeStartTime);
            validateQueryResults(allocator, 
                clientLocation, 
                headerCallOption, 
                new TableRef(CATALOG, SCHEMA, TABLE_NAME), 
                BATCH_COUNT, 
                BATCH_STEP, 
                BATCH_SIZE, 
                VALUE_RATIO,
                ORIGINAL_ROW_COUNT
            );


            // 客户端通过 transaction id 实现 Exactly Once
            FlightSqlClient.Transaction transaction = flightSqlClient.beginTransaction(headerCallOption);
            System.out.println("Transaction ID: " + new String(transaction.getTransactionId(), StandardCharsets.UTF_8));

            long transactionWriteStartTime = System.currentTimeMillis();
            writeBatchStreamly(
                flightSqlClient, 
                transaction, 
                headerCallOption, 
                allocator, 
                tableSchema, 
                TABLE_NAME, 
                CATALOG, 
                SCHEMA, 
                BATCH_SIZE,
                BATCH_COUNT,
                BATCH_COUNT,
                partitionCols,
                primaryKeys,
                cdcColumn
            );
            printTimeElapsed("事务数据写入", transactionWriteStartTime);

            validateQueryResults(allocator, 
                clientLocation, 
                headerCallOption, 
                new TableRef(CATALOG, SCHEMA, TABLE_NAME), 
                BATCH_COUNT, 
                BATCH_STEP, 
                BATCH_SIZE, 
                VALUE_RATIO,
                ORIGINAL_ROW_COUNT
            );
            long transactionCommitStartTime = System.currentTimeMillis();
//            flightSqlClient.rollback(transaction, headerCallOption);
            flightSqlClient.commit(transaction, headerCallOption);
            printTimeElapsed("事务提交", transactionCommitStartTime);

        }


        // 替换原有的查询代码为新的方法调用
        validateQueryResults(
            allocator, 
            clientLocation, 
            headerCallOption,
            new TableRef(CATALOG, SCHEMA, TABLE_NAME),
            BATCH_COUNT * 2,
            BATCH_STEP,
            BATCH_SIZE,
            VALUE_RATIO,
            ORIGINAL_ROW_COUNT
        );
    }

    static void writeBatch(VectorSchemaRoot batch, ArrowStreamWriter writer, int batchSize, int batchCount, int batchOffset, List<String> partitionCols, List<String> primaryKeys, String cdcColumn) throws Exception {
        long batchWriteStartTime = System.currentTimeMillis();
        batch.allocateNew();
        
        for (int i = batchOffset; i < batchOffset + batchCount; ++i) {
            batch.clear();
            
            // 根据 schema 动态处理每个字段
            for (FieldVector vector : batch.getFieldVectors()) {
                String fieldName = vector.getField().getName();
                boolean isPartitionCol = partitionCols != null && partitionCols.contains(fieldName);
                boolean isCdcColumn = cdcColumn != null && fieldName.equals(cdcColumn);
                boolean isPrimaryKey = primaryKeys != null && primaryKeys.contains(fieldName);
                
                switch (vector.getField().getType().getTypeID()) {
                    case Utf8:
                        VarCharVector stringVector = (VarCharVector) vector;
                        stringVector.allocateNew((long) batchSize * 3 * MAX_STRING_LENGTH, batchSize * 3);
                        for (int j = 0; j < batchSize * 2; j++) {
                            String value;
                            if (isCdcColumn) {
                                value = "insert";
                            } else if (isPartitionCol) {
                                value = "partition_" + (j % 2);
                            } else {
                                value = "test_string_" + j + i * BATCH_STEP * 2;
                            }
                            stringVector.set(j, value.getBytes(StandardCharsets.UTF_8));
                        }
                        for (int j = 0; j < batchSize; j++) {
                            String value;
                            if (isCdcColumn) {
                                value = "delete";
                            } else if (isPartitionCol) {
                                value = "partition_" + (j % 2);
                            } else {
                                value = "test_string_" + j + i;
                            }
                            stringVector.set(j + batchSize * 2, value.getBytes(StandardCharsets.UTF_8));
                        }
                        break;
                    case Int:
                        IntVector intVector = (IntVector) vector;
                        intVector.allocateNew(batchSize * 3);
                        if (isPrimaryKey || isPartitionCol) {
                            for (int j = 0; j < batchSize * 2; j++) {
                                intVector.set(j, i * BATCH_STEP * 2 + j);
                            }
                            for (int j = 0; j < batchSize; j++) {
                                intVector.set(j + batchSize * 2, i * BATCH_STEP * 2 + j);
                            }
                        } else {
                            for (int j = 0; j < batchSize * 3; j++) {
                                intVector.set(j, isPartitionCol ? 
                                    j % 2 : 
                                    i * BATCH_STEP * 2 + j);
                            }
                        }
                        break;
                    case FloatingPoint:
                        FloatingPointVector floatVector = (FloatingPointVector) vector;
                        floatVector.allocateNew();
                        for (int j = 0; j < batchSize * 3; j++) {
                            floatVector.setWithPossibleTruncate(j, isPartitionCol ? 
                                j % 2 : 
                                (i * BATCH_STEP * 2 + j) * VALUE_RATIO);
                        }
                        break;
                    case Decimal:
                        DecimalVector decimalVector = (DecimalVector) vector;
                        decimalVector.allocateNew(batchSize * 3);
                        for (int j = 0; j < batchSize * 3; j++) {
                            decimalVector.set(j, ((long) i * BATCH_STEP * 2 + j) * (long) Math.pow(10, decimalVector.getScale()));
                        }
                        break;
                    case Date:
                        DateDayVector dateVector = (DateDayVector) vector;
                        dateVector.allocateNew(batchSize * 3);
                        if (isPrimaryKey || isPartitionCol) {
                            for (int j = 0; j < batchSize * 2; j++) {
                                int date = isPartitionCol ? 
                                    (i * BATCH_STEP * 2 + j) % 3 : 
                                    (i * BATCH_STEP * 2 + j);
                                dateVector.set(j, date);
                            }
                            for (int j = 0; j < batchSize; j++) {
                                int date = isPartitionCol ? 
                                    (i * BATCH_STEP * 2 + j) % 3 : 
                                    (i * BATCH_STEP * 2 + j);
                                dateVector.set(j + batchSize * 2, date);
                            }
                        } else {
                            for (int j = 0; j < batchSize * 3; j++) {
                                int date = isPartitionCol ? 
                                    (i * BATCH_STEP * 2 + j) % 3 : 
                                    (i * BATCH_STEP * 2 + j);
                            }
                        }
                        break;
                    case Timestamp:
                        if (vector instanceof TimeStampMicroTZVector) {
                            TimeStampMicroTZVector timestampVector = (TimeStampMicroTZVector) vector;
                            timestampVector.allocateNew(batchSize * 3);
                            for (int j = 0; j < batchSize * 3; j++) {
                                // 转换为微秒级时间戳
                                timestampVector.set(j, (i * BATCH_STEP * 2 + j) * 1000L);
                            }
                        } else if (vector instanceof TimeMilliVector) {
                            TimeMilliVector timeMilliVector = (TimeMilliVector) vector;
                            timeMilliVector.allocateNew(batchSize * 3);
                            for (int j = 0; j < batchSize * 3; j++) {
                                timeMilliVector.set(j, (i * BATCH_STEP * 2 + j));
                            }
                        } else {
                            throw new IllegalArgumentException("不支持的时间戳类型: " + vector.getClass().getName());
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("不支持的类型: " + vector.getField().getType().getTypeID());
                }
            }
            
            batch.setRowCount(batchSize * 3);
            System.out.println(batch.contentToTSVString());
            writer.writeBatch();
        }
        printTimeElapsed("批次写入", batchWriteStartTime);
    }

    private static void printTimeElapsed(String operation, long startTime) {
        long endTime = System.currentTimeMillis();
        System.out.println(operation + " 耗时: " + (endTime - startTime) + " 毫秒");
    }

    // 新增的查询验证方法
    private static void validateQueryResults(
            BufferAllocator allocator,
            Location clientLocation,
            HeaderCallOption headerCallOption,
            TableRef tableRef,
            int batchCount,
            int batchStep,
            int batchSize,
            float valueRatio,
            int originalRowCount
    ) {
        long queryStartTime = System.currentTimeMillis();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build())) {

            try (FlightSqlClient.PreparedStatement preparedStatement = flightSqlClient.prepare(
                    String.format("select * from %s.%s",
                            tableRef.getDbSchema(), tableRef.getTable()),
                    headerCallOption
            )) {
                FlightInfo flightInfo = preparedStatement.execute();
                int rowCount = 0;
                try (final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                    while (stream.next()) {
                        rowCount += stream.getRoot().getRowCount();
                    }
                }
                System.out.println("总行数: " + rowCount);
            }
            // 查询1：验证总行数
            long query1StartTime = System.currentTimeMillis();
            try (FlightSqlClient.PreparedStatement preparedStatement = flightSqlClient.prepare(
                    String.format("select count(*) as total_count from %s.%s",
                            tableRef.getDbSchema(), tableRef.getTable()),
                    headerCallOption
            )) {
                FlightInfo flightInfo = preparedStatement.execute();
                try (final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                    while (stream.next()) {
                        try (final VectorSchemaRoot root = stream.getRoot()) {
                            long totalCount = ((BigIntVector)root.getVector(0)).get(0);
                            long expectedCount = originalRowCount + batchCount * batchStep + batchSize - batchStep;
                            assertThat(totalCount)
                                .as(String.format("行数不匹配: %d != %d", totalCount, expectedCount))
                                .isEqualTo(expectedCount);
                        }
                    }
                }
            }
            printTimeElapsed("查询1(总行数)", query1StartTime);

        } catch (Exception e) {
            throw new RuntimeException("查询验证失败", e);
        }

        printTimeElapsed("数据查询验证", queryStartTime);
    }

    private static long writeBatchStreamly(
            FlightSqlClient flightSqlClient,
            FlightSqlClient.Transaction transaction,
            HeaderCallOption headerCallOption,
            BufferAllocator allocator,
            Schema tableSchema,
            String tableName,
            String catalog,
            String schema,
            int batchSize,
            int batchCount,
            int batchOffset,
            List<String> partitionCols,
            List<String> primaryKeys,
            String cdcColumn
    ) throws Exception {
        try (final VectorSchemaRoot ingestRoot = VectorSchemaRoot.create(tableSchema, allocator);
             PipedInputStream inPipe = new PipedInputStream(batchSize * 3);
             PipedOutputStream outPipe = new PipedOutputStream(inPipe);
             ArrowStreamReader reader = new ArrowStreamReader(inPipe, allocator)) {

            // 启动后台写入线程
            new Thread(() -> {
                try (ArrowStreamWriter writer = new ArrowStreamWriter(ingestRoot, null, outPipe)) {
                    writer.start();
                    writeBatch(ingestRoot, writer, batchSize, batchCount, batchOffset, partitionCols, primaryKeys, cdcColumn);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();

            // 配置DDL选项
            FlightSql.CommandStatementIngest.TableDefinitionOptions ddlOptions =
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder()
                            .setIfExists(FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
                            .setIfNotExist(FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption.TABLE_NOT_EXIST_OPTION_FAIL)
                            .build();

            // 执行数据摄入
            return flightSqlClient.executeIngest(
                    reader,
                    new FlightSqlClient.ExecuteIngestOptions(
                            tableName,
                            ddlOptions,
                            catalog,
                            schema,
                            null),
                    transaction,
                    headerCallOption);
        }
    }
}

    