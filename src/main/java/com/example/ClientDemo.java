package com.example;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.apache.arrow.flight.auth2.Auth2Constants.AUTHORIZATION_HEADER;
import static org.apache.arrow.flight.auth2.Auth2Constants.BEARER_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;

public class ClientDemo {

    // 连接参数
    public static final String HOST = "localhost";
    public static final int PORT = 50051;
    
    // 数据库相关参数
    public static final String CATALOG = "lakesoul";
    public static final String SCHEMA = "default";
    public static final String TABLE_NAME = "lakesoul_test_table";
    
    // 批处理相关参数
    public static final int BATCH_SIZE = 16;
    public static final int BATCH_COUNT = 32;
    public static final int BATCH_STEP = BATCH_SIZE / 2;
    public static final float VALUE_RATIO = 1.5f;
    public static final int MAX_STRING_LENGTH = 20;
    
    // 验证相关参数
    public static final double FLOAT_COMPARISON_DELTA = 0.0001;

    public static void main(String[] args) throws Exception {

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // 连接域名
        final Location clientLocation = Location.forGrpcInsecure(HOST, PORT);

        // 鉴权 Token Header
        String jwtToken = "";
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(AUTHORIZATION_HEADER, BEARER_PREFIX + jwtToken);
        HeaderCallOption headerCallOption = new HeaderCallOption(headers);

        long writeStartTime = System.currentTimeMillis();

        // 流式写入数据
        try (FlightSqlClient flightSqlClient =
                     new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build())) {

            // 获取目标表的 schema
            Schema tableSchema = flightSqlClient.getSchema(
                    FlightDescriptor.path(SCHEMA, TABLE_NAME), headerCallOption).getSchema();
            System.out.println(tableSchema);

            // 客户端通过 transaction id 实现 Exactly Once
            UUID transactionId = UUID.randomUUID();
            System.out.println(transactionId);
            FlightSqlClient.Transaction transaction =
                    new FlightSqlClient.Transaction(transactionId.toString().getBytes(StandardCharsets.UTF_8));

            // 创建一个 pipe ，能够写入多个 batch
            try (final VectorSchemaRoot ingestRoot = VectorSchemaRoot.create(tableSchema, allocator);
                 PipedInputStream inPipe = new PipedInputStream(BATCH_SIZE);
                 PipedOutputStream outPipe = new PipedOutputStream(inPipe);
                 ArrowStreamReader reader = new ArrowStreamReader(inPipe, allocator)) {

                // 用一个后台线程写入多个 batch
                int batchSize = BATCH_SIZE;
                new Thread(
                        () -> {
                            try (ArrowStreamWriter writer =
                                         new ArrowStreamWriter(ingestRoot, null, outPipe)) {
                                writer.start();
                                writeBatch(ingestRoot, writer, batchSize);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .start();

                // 设置写入时 DDL 行为，基于安全和稳定性考虑，远程写入时不允许自动建表。
                FlightSql.CommandStatementIngest.TableDefinitionOptions
                        ddlOptions =
                        FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder()
                                .setIfExists(
                                        FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
                                .setIfNotExist(
                                        FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption.TABLE_NOT_EXIST_OPTION_FAIL)
                                .build();
                // 客户端将多个 batch 流式发送到服务端，指定本次的 transaction id
                final long updatedRows =
                        flightSqlClient.executeIngest(
                                reader,
                                new FlightSqlClient.ExecuteIngestOptions(
                                        TABLE_NAME,
                                        ddlOptions,
                                        CATALOG,
                                        SCHEMA,
                                        null),
                                transaction,
                                headerCallOption);
                System.out.println(updatedRows);
            }
        }

        printTimeElapsed("数据写入", writeStartTime);

        // 流式读一个表
        long queryStartTime = System.currentTimeMillis();

        try (FlightSqlClient flightSqlClient =
                     new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build())) {
//            Schema tableSchema = flightSqlClient.getSchema(
//                    FlightDescriptor.command(FlightSql.CommandGetDbSchemas.newBuilder()
//                            .setCatalog("default")
//                            .setDbSchemaFilterPattern("lakesoul_test_table")
//                            .build()
//                            .toByteArray()), headerCallOption).getSchema();
            try (FlightSqlClient.PreparedStatement preparedStatement = flightSqlClient.prepare(
                    "select count(*) as total_count from default.lakesoul_test_table", headerCallOption
            )) {
                // 首先验证总行数
                FlightInfo flightInfo = preparedStatement.execute();
                final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), headerCallOption);
                while (stream.next()) {
                    try (final VectorSchemaRoot root = stream.getRoot()) {
                        System.out.println(root.contentToTSVString());
                        long totalCount = ((BigIntVector)root.getVector(0)).get(0);
                        long expectedCount = BATCH_COUNT * BATCH_STEP + BATCH_SIZE - BATCH_STEP;
                        assertThat(totalCount)
                            .as("行数不匹配")
                            .isEqualTo(expectedCount);
                    }
                }
                stream.close();
            }
            printTimeElapsed("查询1(总行数)", queryStartTime);

            // 验证数值范围
            long query2StartTime = System.currentTimeMillis();
            try (FlightSqlClient.PreparedStatement preparedStatement = flightSqlClient.prepare(
                    "select min(score) as min_val, max(score) as max_val from default.lakesoul_test_table", headerCallOption
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
                            .isLessThan((BATCH_COUNT * BATCH_STEP + BATCH_SIZE - BATCH_STEP) * VALUE_RATIO);
                    }
                }
                stream.close();
            }
            printTimeElapsed("查询2(数值范围)", query2StartTime);

            // 验证数据内的正确性
            long query3StartTime = System.currentTimeMillis();
            try (FlightSqlClient.PreparedStatement preparedStatement = flightSqlClient.prepare(
                    String.format("select * from default.lakesoul_test_table where score = %f", VALUE_RATIO), headerCallOption
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
                            .isCloseTo(VALUE_RATIO, within(FLOAT_COMPARISON_DELTA));
                    }
                }
                stream.close();
            }
            printTimeElapsed("查询3(数据正确性)", query3StartTime);
        }

        printTimeElapsed("数据查询验证", queryStartTime);
    }

    static void writeBatch(VectorSchemaRoot batch, ArrowStreamWriter writer, int batchSize) throws Exception {
        long batchWriteStartTime = System.currentTimeMillis();
        batch.allocateNew();
        // 写入 100 个 batch
        for (int i = 0; i < BATCH_COUNT; ++i) {
            batch.clear();
            final VarCharVector stringVector = (VarCharVector) batch.getVector(0);
            final IntVector intVector = (IntVector) batch.getVector(1);
            final FloatingPointVector floatVector = (FloatingPointVector) batch.getVector(2);
            
            // 修改：为变长字符串预分配足够的空间
            int maxStringLength = MAX_STRING_LENGTH; // 估计每个字符串的最大长度
            stringVector.allocateNew((long) batchSize * maxStringLength, batchSize);
            intVector.allocateNew(batchSize);
            floatVector.allocateNew();
            
            for (int j = 0; j < batchSize; ++j) {
                stringVector.set(j, ("test_string_" + j + i).getBytes(StandardCharsets.UTF_8));
                intVector.set(j,  i * BATCH_STEP + j);
                floatVector.setWithPossibleTruncate(j, (i * BATCH_STEP + j) * VALUE_RATIO);
            }
            batch.setRowCount(batchSize);
//            System.out.println(batch.contentToTSVString());
            writer.writeBatch();

        }
        printTimeElapsed("批次写入", batchWriteStartTime);
    }

    private static void printTimeElapsed(String operation, long startTime) {
        long endTime = System.currentTimeMillis();
        System.out.println(operation + " 耗时: " + (endTime - startTime) + " 毫秒");
    }
}

    