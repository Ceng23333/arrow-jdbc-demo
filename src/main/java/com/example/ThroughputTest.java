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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.arrow.flight.auth2.Auth2Constants.AUTHORIZATION_HEADER;
import static org.apache.arrow.flight.auth2.Auth2Constants.BEARER_PREFIX;

public class ThroughputTest {
    // 连接参数
    private static final String HOST = "dapm-api.dmetasoul.com";
    private static final int PORT = 443;
    
    // 测试参数
    private static final int CONCURRENT_CLIENTS = 8;  // 并发客户端数
    private static final int BATCH_SIZE = 10000;      // 每个批次的行数
    private static final int BATCH_COUNT = 128;       // 每个客户端的批次数
    private static final int PIPE_BUFFER_SIZE = 1024 * 1024;  // 管道缓冲区大小
    
    // 性能统计
    private static final AtomicLong totalRows = new AtomicLong(0);
    private static final AtomicLong totalBytes = new AtomicLong(0);
    
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        // 鉴权 Token Header
        String jwtToken = "";
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(AUTHORIZATION_HEADER, BEARER_PREFIX + jwtToken);
        HeaderCallOption headerCallOption = new HeaderCallOption(headers);
        
        try (BufferAllocator allocator = new RootAllocator()) {
            // 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
            List<Future<ClientStats>> futures = new ArrayList<>();
            
            // 启动多个客户端
            for (int i = 0; i < CONCURRENT_CLIENTS; i++) {
                final int clientId = i;
                futures.add(executor.submit(() -> runClient(clientId, allocator, headerCallOption)));
            }
            
            // ���待所有客户端完成并收集统计信息
            for (Future<ClientStats> future : futures) {
                ClientStats stats = future.get();
                totalRows.addAndGet(stats.rows);
                totalBytes.addAndGet(stats.bytes);
            }
            
            // 关闭线程池
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);
        }
        
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        
        // 打印性能统计
        System.out.println("\n性能测试结果:");
        System.out.printf("总时间: %.2f 秒\n", duration);
        System.out.printf("总行数: %d\n", totalRows.get());
        System.out.printf("总数据量: %.2f MB\n", totalBytes.get() / (1024.0 * 1024.0));
        System.out.printf("平均吞吐量: %.2f 行/秒\n", totalRows.get() / duration);
        System.out.printf("平均数据速率: %.2f MB/秒\n", (totalBytes.get() / (1024.0 * 1024.0)) / duration);
    }
    
    private static ClientStats runClient(int clientId, BufferAllocator allocator, HeaderCallOption headerCallOption) {
        ClientStats stats = new ClientStats();
        long clientStartTime = System.currentTimeMillis();
        
        // 使用互质的乘数数组
        int[] coprimeMultipliers = {3, 5, 7, 11};  // 这些数互质
        int clientBatchCount = BATCH_COUNT * coprimeMultipliers[clientId % 4];  // 每个客户端使用不同的互质乘数
        
        try {
//            Location location = Location.forGrpcInsecure(HOST, PORT);
            Location location = Location.forGrpcTls(HOST, PORT);
            try (FlightClient flightClient = FlightClient.builder(allocator, location).build();
                 FlightSqlClient client = new FlightSqlClient(flightClient)) {

                String tableName = String.format("throughput_test_%d", clientId);
                long code;
                code = client.executeUpdate(String.format("DROP TABLE IF EXISTS %s", tableName), headerCallOption);
                System.out.println("drop table return code: " + code);
//                code = client.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS %s (name STRING, id INT PRIMARY KEY, score FLOAT)", tableName), headerCallOption);
                code = client.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS %s  (name STRING, id INT, score FLOAT)", tableName), headerCallOption);
                System.out.println("create table return code: " + code);
                
                // 获取表结构
                FlightInfo primaryKeysInfo = client.getPrimaryKeys(
                    TableRef.of("lakesoul", "default", "throughput_test_" + clientId),
                    null,
                    null
                );
                Schema tableSchema = primaryKeysInfo.getSchemaOptional().get();
                
                // 写入数据
                stats.bytes = writeBatchStreamly(
                    client,
                    null,
                    null,
                    allocator,
                    tableSchema,
                    "throughput_test_" + clientId,
                    "lakesoul",
                    "default",
                    BATCH_SIZE,
                    clientBatchCount,  // 使用互质倍数的批次数
                    clientId * clientBatchCount * BATCH_SIZE
                );
                
                stats.rows = (long) BATCH_SIZE * clientBatchCount;
            }
        } catch (Exception e) {
            System.err.printf("客户端 %d 发生错误: %s\n", clientId, e.getMessage());
            e.printStackTrace();
        }
        
        long clientEndTime = System.currentTimeMillis();
        System.out.printf("客户端 %d 完成, 批次数: %d (原始批次数 × %d), 耗时: %.2f 秒\n", 
            clientId, clientBatchCount, coprimeMultipliers[clientId % 4], (clientEndTime - clientStartTime) / 1000.0);
        
        return stats;
    }
    
    private static long writeBatchStreamly(
            FlightSqlClient client,
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
        try (VectorSchemaRoot ingestRoot = VectorSchemaRoot.create(tableSchema, allocator);
             PipedInputStream inPipe = new PipedInputStream(PIPE_BUFFER_SIZE);
             PipedOutputStream outPipe = new PipedOutputStream(inPipe);
             ArrowStreamReader reader = new ArrowStreamReader(inPipe, allocator)) {

            CountDownLatch writerLatch = new CountDownLatch(1);
            AtomicLong bytesWritten = new AtomicLong(0);

            // 启动写入线程
            new Thread(() -> {
                try (ArrowStreamWriter writer = new ArrowStreamWriter(ingestRoot, null, outPipe)) {
                    writer.start();
                    writeLargeBatch(ingestRoot, writer, batchSize, batchCount, batchOffset, bytesWritten);
                    writerLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            // 配置DDL选项
            FlightSql.CommandStatementIngest.TableDefinitionOptions ddlOptions =
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder()
                            .setIfExists(FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
                            .setIfNotExist(FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption.TABLE_NOT_EXIST_OPTION_CREATE)
                            .build();

            // 执行数据摄入
            client.executeIngest(
                reader,
                new FlightSqlClient.ExecuteIngestOptions(
                    tableName,
                    ddlOptions,
                    catalog,
                    schema,
                    null),
                transaction,
                headerCallOption);

            writerLatch.await();
            return bytesWritten.get();
        }
    }
    
    private static void writeLargeBatch(
            VectorSchemaRoot batch,
            ArrowStreamWriter writer,
            int batchSize,
            int batchCount,
            int batchOffset,
            AtomicLong bytesWritten
    ) throws Exception {
        batch.allocateNew();
        
        for (int i = batchOffset; i < batchOffset + batchCount; i++) {
            batch.clear();
            VarCharVector stringVector = (VarCharVector) batch.getVector(0);
            IntVector intVector = (IntVector) batch.getVector(1);
            Float4Vector floatVector = (Float4Vector) batch.getVector(2);
            
            stringVector.allocateNew(batchSize * 20, batchSize);
            intVector.allocateNew(batchSize);
            floatVector.allocateNew(batchSize);
            
            for (int j = 0; j < batchSize; j++) {
                String str = String.format("test_string_%d_%d", i, j);
                stringVector.setSafe(j, str.getBytes(StandardCharsets.UTF_8));
                intVector.setSafe(j, i * batchSize + j);
                floatVector.set(j, (i * batchSize + j) * 1.5f);
                
                bytesWritten.addAndGet(str.length() + 12);
            }
            
            batch.setRowCount(batchSize);
            writer.writeBatch();
        }
    }
    
    private static class ClientStats {
        long rows;
        long bytes;
    }
}