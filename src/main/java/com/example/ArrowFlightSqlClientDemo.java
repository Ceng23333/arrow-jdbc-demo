package com.example;

import org.apache.arrow.adbc.core.*;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ArrowFlightSqlClientDemo {
    private static final String HOST = "localhost";
    private static final int PORT = 50051;
    
    public static void main(String[] args) {
        try (RootAllocator allocator = new RootAllocator()) {

            
            // 创建 ADBC 数据库连接
            String url = String.format("jdbc:arrow-flight-sql://localhost:%d?useEncryption=false", PORT);
            final Map<String, Object> parameters = new HashMap<>();
            parameters.put(AdbcDriver.PARAM_URI.getKey(), url);
            parameters.put("useEncryption", "false");
            try (
                    AdbcDatabase db = new JdbcDriver(allocator).open(parameters);
                    AdbcConnection adbcConnection = db.connect();
            ) {
                
                System.out.println("成功连接到 Arrow Flight SQL 服务器");
                
                // 创建表
                try (AdbcStatement statement = adbcConnection.createStatement()) {
                    statement.setSqlQuery("CREATE EXTERNAL TABLE IF NOT EXISTS test_table (" +
                            "id INTEGER PRIMARY KEY, " +
                            "name VARCHAR(50), " +
                            "age INTEGER) STORED AS LAKESOUL LOCATION ''");
                    statement.executeUpdate();
                    System.out.println("表创建成功");
                    
                    // 插入数据
                    insertDataWithVectorSchemaRoot(adbcConnection, allocator);
                    
                    // 查询数据
                    statement.setSqlQuery("SELECT * FROM test_table");
                    AdbcStatement.QueryResult queryResult = statement.executeQuery();
                    while (queryResult.getReader().loadNextBatch()) {
                        // process batch
                        VectorSchemaRoot root = queryResult.getReader().getVectorSchemaRoot();
                        System.out.println(root.contentToTSVString());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("发生错误：" + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void insertDataWithVectorSchemaRoot(AdbcConnection connection, RootAllocator allocator) throws Exception {
        // 定义 schema
        Schema schema = new Schema(Arrays.asList(
            Field.nullable("id", new ArrowType.Int(32, true)),
            Field.nullable("name", new ArrowType.Utf8()),
            Field.nullable("age", new ArrowType.Int(32, true))
        ));
        
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            // 获取向量
            IntVector idVector = (IntVector) root.getVector("id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector ageVector = (IntVector) root.getVector("age");
            
            // 分配内存
            idVector.allocateNew(3);
            nameVector.allocateNew(3);
            ageVector.allocateNew(3);
            
            // 设置数据
            idVector.setSafe(0, 1);
            idVector.setSafe(1, 2);
            idVector.setSafe(2, 3);
            
            nameVector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
            nameVector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
            nameVector.setSafe(2, "Charlie".getBytes(StandardCharsets.UTF_8));
            
            ageVector.setSafe(0, 25);
            ageVector.setSafe(1, 30);
            ageVector.setSafe(2, 35);
            
            // 设置行数
            root.setRowCount(3);
            
            // 使用 bulkIngest 插入数据
            try (AdbcStatement stmt = connection.bulkIngest("test_table", BulkIngestMode.CREATE_APPEND)) {
                stmt.bind(root);
                stmt.executeUpdate();
            }
            
            System.out.println("数据插入成功");
        }
    }
} 