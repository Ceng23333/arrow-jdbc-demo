package com.example;

import com.example.util.TablePartitionUtil;
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

public class ClientDemo {

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
    public static final int BATCH_SIZE = 16;
    public static final int BATCH_COUNT = 16;
    public static final int BATCH_STEP = BATCH_SIZE / 2;
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
            code = flightSqlClient.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS %s (name STRING, id INT PRIMARY KEY, score FLOAT, date DATE, region STRING) PARTITION BY (region, date)", TABLE_NAME), headerCallOption);
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

            List<String> partitionCols = TablePartitionUtil.parseTableInfoPartitions(metadataMap.get("partitions").toString()).getRangeKeys();
            System.out.println(partitionCols);

            
            Schema tableSchema = primaryKeysInfo.getSchemaOptional().get();
            System.out.println(tableSchema);
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
                partitionCols
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
                partitionCols
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

    static void writeBatch(VectorSchemaRoot batch, ArrowStreamWriter writer, int batchSize, int batchCount, int batchOffset, List<String> partitionCols) throws Exception {
        long batchWriteStartTime = System.currentTimeMillis();
        batch.allocateNew();
        
        for (int i = batchOffset; i < batchOffset + batchCount; ++i) {
            batch.clear();
            
            // 根据 schema 动态处理每个字段
            for (FieldVector vector : batch.getFieldVectors()) {
                String fieldName = vector.getField().getName();
                boolean isPartitionCol = partitionCols != null && partitionCols.contains(fieldName);
                
                switch (vector.getField().getType().getTypeID()) {
                    case Utf8:
                        VarCharVector stringVector = (VarCharVector) vector;
                        stringVector.allocateNew((long) batchSize * MAX_STRING_LENGTH, batchSize);
                        for (int j = 0; j < batchSize; j++) {
                            String value = isPartitionCol ? 
                                "partition_" + (j % 2) : 
                                "test_string_" + j + i;
                            stringVector.set(j, value.getBytes(StandardCharsets.UTF_8));
                        }
                        break;
                    case Int:
                        IntVector intVector = (IntVector) vector;
                        intVector.allocateNew(batchSize);
                        for (int j = 0; j < batchSize; j++) {
                            intVector.set(j, isPartitionCol ? 
                                j % 2 : 
                                i * BATCH_STEP + j);
                        }
                        break;
                    case FloatingPoint:
                        FloatingPointVector floatVector = (FloatingPointVector) vector;
                        floatVector.allocateNew();
                        for (int j = 0; j < batchSize; j++) {
                            floatVector.setWithPossibleTruncate(j, isPartitionCol ? 
                                j % 2 : 
                                (i * BATCH_STEP + j) * VALUE_RATIO);
                        }
                        break;
                    case Decimal:
                        DecimalVector decimalVector = (DecimalVector) vector;
                        decimalVector.allocateNew(batchSize);
                        for (int j = 0; j < batchSize; j++) {
                            decimalVector.set(j, ((long) i * BATCH_STEP + j) * (long) Math.pow(10, decimalVector.getScale()));
                        }
                        break;
                    case Date:
                        DateDayVector dateVector = (DateDayVector) vector;
                        dateVector.allocateNew(batchSize);
                        for (int j = 0; j < batchSize; j++) {
                            int date = isPartitionCol ? 
                                (i * BATCH_STEP + j) % 3 : 
                                (i * BATCH_STEP + j);
                            dateVector.set(j, date);
                        }
                        break;
                    case Timestamp:
                        if (vector instanceof TimeStampMicroTZVector) {
                            TimeStampMicroTZVector timestampVector = (TimeStampMicroTZVector) vector;
                            timestampVector.allocateNew(batchSize);
                            for (int j = 0; j < batchSize; j++) {
                                // 转换为微秒级时间戳
                                timestampVector.set(j, (i * BATCH_STEP + j) * 1000L);
                            }
                        } else if (vector instanceof TimeMilliVector) {
                            TimeMilliVector timeMilliVector = (TimeMilliVector) vector;
                            timeMilliVector.allocateNew(batchSize);
                            for (int j = 0; j < batchSize; j++) {
                                timeMilliVector.set(j, (i * BATCH_STEP + j));
                            }
                        } else {
                            throw new IllegalArgumentException("不支持的时间戳类型: " + vector.getClass().getName());
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("不支持的类型: " + vector.getField().getType().getTypeID());
                }
            }
            
            batch.setRowCount(batchSize);
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
                            System.out.println(root.contentToTSVString());
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
//            if (1==1) return;

            // 查询2：验证数值范围
            long query2StartTime = System.currentTimeMillis();
            try (FlightSqlClient.PreparedStatement preparedStatement = flightSqlClient.prepare(
                    String.format("select min(score) as min_val, max(score) as max_val from %s.%s",
                            tableRef.getDbSchema(), tableRef.getTable()),
                    headerCallOption
            )) {
                FlightInfo flightInfo = preparedStatement.execute();
                final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), headerCallOption);
                while (stream.next()) {
                    try (final VectorSchemaRoot root = stream.getRoot()) {
                        System.out.println(root.contentToTSVString());
                        float minVal = ((Float4Vector)root.getVector(0)).get(0);
                        float maxVal = ((Float4Vector)root.getVector(1)).get(0);
                        
                        // 验证最小值和最大值是否在预期范围内
                        assertThat(minVal)
                            .as("最小值应该大于等于0")
                            .isGreaterThanOrEqualTo(0);
                        assertThat(maxVal)
                            .as("最大值应该在预期范围内")
                            .isLessThan((batchCount * batchStep + batchSize - batchStep) * valueRatio);
                    }
                }
                stream.close();
            }
            printTimeElapsed("查询2(数值范围)", query2StartTime);

            // 查询3：验证数据正确性
            long query3StartTime = System.currentTimeMillis();
            try (FlightSqlClient.PreparedStatement preparedStatement = flightSqlClient.prepare(
                    String.format("select * from %s.%s where score = %f",
                            tableRef.getDbSchema(), tableRef.getTable(), valueRatio),
                    headerCallOption
            )) {
                FlightInfo flightInfo = preparedStatement.execute();
                final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), headerCallOption);
                while (stream.next()) {
                    try (final VectorSchemaRoot root = stream.getRoot()) {
                        System.out.println(root.contentToTSVString());
                        // 验证字符串格式
                        VarCharVector stringVector = (VarCharVector) root.getVector(0);
                        String value = new String(stringVector.get(0), StandardCharsets.UTF_8);
                        assertThat(value)
                            .as("字符串格式不正确")
                            .startsWith("test_string_");
                        
                        // 验证整数值
                        IntVector intVector = (IntVector) root.getVector(1);
                        assertThat(intVector.get(0))
                            .as("整数值不匹配")
                            .isEqualTo(1);
                        
                        // 验证浮数值
                        FloatingPointVector floatVector = (FloatingPointVector) root.getVector(2);
                        assertThat(floatVector.getValueAsDouble(0))
                            .as("浮点数值不匹配")
                            .isCloseTo(valueRatio, within(FLOAT_COMPARISON_DELTA));
                    }
                }
                stream.close();
            }
            printTimeElapsed("查询3(数据正确性)", query3StartTime);
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
            List<String> partitionCols
    ) throws Exception {
        try (final VectorSchemaRoot ingestRoot = VectorSchemaRoot.create(tableSchema, allocator);
             PipedInputStream inPipe = new PipedInputStream(batchSize);
             PipedOutputStream outPipe = new PipedOutputStream(inPipe);
             ArrowStreamReader reader = new ArrowStreamReader(inPipe, allocator)) {

            // 启动后台写入线程
            new Thread(() -> {
                try (ArrowStreamWriter writer = new ArrowStreamWriter(ingestRoot, null, outPipe)) {
                    writer.start();
                    writeBatch(ingestRoot, writer, batchSize, batchCount, batchOffset, partitionCols);
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

    