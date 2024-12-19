package com.example;

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
    public static final int BATCH_SIZE = 1024;
    public static final int BATCH_COUNT = 512;
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


        // 流式写入数据
        try (FlightSqlClient flightSqlClient =
                     new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build())) {

            // 测试连接是否正常
            FlightInfo selectOneInfo = flightSqlClient.execute("SELECT 1", headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(selectOneInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("SELECT 1 result: " + root.getFieldVectors().get(0).getObject(0));
                }
            }

//            // 测试获取 catalog 列表
//            FlightInfo catalogsInfo = flightSqlClient.getCatalogs(headerCallOption);
//            try (FlightStream stream = flightSqlClient.getStream(catalogsInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
//                VectorSchemaRoot root = stream.getRoot();
//                while (stream.next()) {
//                    System.out.println("Catalog: " + root.getFieldVectors().get(0).getObject(0));
//                }
//            }

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
                0
            );
            printTimeElapsed("无事务数据写入", writeStartTime);
            validateQueryResults(allocator, 
                clientLocation, 
                headerCallOption, 
                new TableRef(CATALOG, SCHEMA, TABLE_NAME), 
                BATCH_COUNT, 
                BATCH_STEP, 
                BATCH_SIZE, 
                VALUE_RATIO
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
                BATCH_COUNT
            );
            printTimeElapsed("事务数据写入", transactionWriteStartTime);

            validateQueryResults(allocator, 
                clientLocation, 
                headerCallOption, 
                new TableRef(CATALOG, SCHEMA, TABLE_NAME), 
                BATCH_COUNT, 
                BATCH_STEP, 
                BATCH_SIZE, 
                VALUE_RATIO
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
            VALUE_RATIO
        );
    }

    static void writeBatch(VectorSchemaRoot batch, ArrowStreamWriter writer, int batchSize, int batchCount, int batchOffset) throws Exception {
        long batchWriteStartTime = System.currentTimeMillis();
        batch.allocateNew();
        // 写入 100 个 batch
        for (int i = batchOffset; i < batchOffset + batchCount; ++i) {
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

    // 新增的查询验证方法
    private static void validateQueryResults(
            BufferAllocator allocator,
            Location clientLocation,
            HeaderCallOption headerCallOption,
            TableRef tableRef,
            int batchCount,
            int batchStep,
            int batchSize,
            float valueRatio
    ) {
        long queryStartTime = System.currentTimeMillis();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build())) {
            // 查询1：验证总���数
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
                            long expectedCount = batchCount * batchStep + batchSize - batchStep;
                            assertThat(totalCount)
                                .as("行数不匹配")
                                .isEqualTo(expectedCount);
                        }
                    }
                }
            }
            printTimeElapsed("查询1(总行数)", query1StartTime);

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
            int batchOffset
    ) throws Exception {
        try (final VectorSchemaRoot ingestRoot = VectorSchemaRoot.create(tableSchema, allocator);
             PipedInputStream inPipe = new PipedInputStream(batchSize);
             PipedOutputStream outPipe = new PipedOutputStream(inPipe);
             ArrowStreamReader reader = new ArrowStreamReader(inPipe, allocator)) {

            // 启动后台写入线程
            new Thread(() -> {
                try (ArrowStreamWriter writer = new ArrowStreamWriter(ingestRoot, null, outPipe)) {
                    writer.start();
                    writeBatch(ingestRoot, writer, batchSize, batchCount, batchOffset);
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

    