package com.example;

import com.example.util.TablePartitionUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.arrow.flight.auth2.Auth2Constants.AUTHORIZATION_HEADER;
import static org.apache.arrow.flight.auth2.Auth2Constants.BEARER_PREFIX;

public class QueryRegressionTest {

    // 连接参数
    public static final String HOST = "localhost";
    public static final int PORT = 50051;
//    public static final String HOST = "dapm-api.dmetasoul.com";
//    public static final int PORT = 443;

    
    // 数据库相关参数
    public static final String CATALOG = "lakesoul";
    public static final String SCHEMA = "default";
    public static final String TABLE_NAME = "test_pk_types";

    
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

            // 测试查询表内容
            String sql = String.format("SELECT * FROM %s", TABLE_NAME);
//            String sql = String.format("SELECT * FROM %s limit 9", TABLE_NAME);
//            String sql = String.format("SELECT order_id FROM %s", TABLE_NAME);
//            String sql = String.format("SELECT order_id, sum(score) FROM %s group by order_id", TABLE_NAME);
            FlightInfo selectAllInfo = flightSqlClient.execute(sql, headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(selectAllInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                System.out.printf("sql: %s \n result: %n", sql);
                while (stream.next()) {
                    System.out.println(root.contentToTSVString());
                }
            }
//            long code;
//            code = flightSqlClient.executeUpdate(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME), headerCallOption);
//            System.out.println("drop table return code: " + code);
//            code = flightSqlClient.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS %s (name STRING, id INT PRIMARY KEY, score FLOAT)", TABLE_NAME), headerCallOption);
//            System.out.println("create table return code: " + code);

            // 测试获取指定 catalog 下的 schema 列表
            FlightInfo schemasInfo = flightSqlClient.getSchemas(CATALOG, null, headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(schemasInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("Schema: " + root.contentToTSVString());
                }
            }

            // 测试获取指定 catalog 和 schema 下的表列表
            FlightInfo tablesInfo = flightSqlClient.getTables(CATALOG, SCHEMA, null, null, false, headerCallOption);
            try (FlightStream stream = flightSqlClient.getStream(tablesInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    System.out.println("Table: " + root.contentToTSVString());
                }
            }

            FlightInfo primaryKeysInfo = flightSqlClient.getPrimaryKeys(TableRef.of(CATALOG, SCHEMA, TABLE_NAME), null, headerCallOption);
            String metadata = new String(primaryKeysInfo.getAppMetadata(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> metadataMap = mapper.readValue(metadata, new TypeReference<HashMap<String, Object>>() {
            });

            List<String> partitionCols = TablePartitionUtil.parseTableInfoPartitions(metadataMap.get("partitions").toString()).getRangeKeys();


            Schema tableSchema = primaryKeysInfo.getSchemaOptional().get();
            System.out.println(tableSchema);
//            try (FlightStream stream = flightSqlClient.getStream(primaryKeysInfo.getEndpoints().get(0).getTicket(), headerCallOption)) {
//                VectorSchemaRoot root = stream.getRoot();
//                while (stream.next()) {
//                    System.out.println("Primary Key: " + root.getFieldVectors().get(0).getObject(0));
//                }
//            }


        }
    }

}

    