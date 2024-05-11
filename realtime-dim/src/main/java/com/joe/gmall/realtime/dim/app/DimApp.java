package com.joe.gmall.realtime.dim.app;


import com.alibaba.fastjson.JSONObject;
import com.joe.gmall.realtime.common.app.BaseAPP;
import com.joe.gmall.realtime.common.bean.TableProcessDim1;
import com.joe.gmall.realtime.common.constant.Constant;
import com.joe.gmall.realtime.common.util.FlinkSourceUtil;
import com.joe.gmall.realtime.common.util.HBaseUtil;
import com.joe.gmall.realtime.dim.function.DimBroadcastFunction;
import com.joe.gmall.realtime.dim.function.DimHBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DimApp extends BaseAPP {
    public static void main(String[] args) {
        new DimApp().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务逻辑
        // 1. 对ods读取的原始数据进行数据清洗
//        stream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String value) throws Exception {
//                boolean flat = false;
//                try {
//                    JSONObject jsonObject = JSONObject.parseObject(value);
//                    String database = jsonObject.getString("database");
//                    String type = jsonObject.getString("type");
//                    JSONObject data = jsonObject.getJSONObject("data");
//                    if ("gmall".equals(database) &&
//                            !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)
//                            && data != null && data.size() != 0){
//                        flat = true;
//                    }
//                }catch (Exception e){
//                    e.printStackTrace();
//                }
//                return flat;
//            }
//        }).map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 2. 使用flinkCDC读取监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
//        mysqlSource.print();

        // 3. 在HBase中创建维度表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHBaseTable(mysqlSource).setParallelism(1);

//        createTableStream.print();

        // 4. 做成广播流
        // 广播状态的key用于判断是否是维度表   value用于补充信息写出到HBase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTableStream.broadcast(broadcastState);

        // 5. 连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectionStream(jsonObjStream, broadcastState, broadcastStateStream);

//        dimStream.print();
        // 6. 筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = filterColumn(dimStream);

        filterColumnStream.print();
        // 7. 写出到HBase
        filterColumnStream.addSink(new DimHBaseSinkFunction());

    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumn(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDim dim = value.f1;
                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObj.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream(SingleOutputStreamOperator<JSONObject> jsonObjStream, MapStateDescriptor<String, TableProcessDim> broadcastState, BroadcastStream<TableProcessDim> broadcastStateStream) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjStream.connect(broadcastStateStream);

        return connectStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1);
    }

    public SingleOutputStreamOperator<TableProcessDim> createHBaseTable(DataStreamSource<String> mysqlSource) {
        return mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
                public Connection connection;

                @Override
                public void open(Configuration parameters) throws Exception {
                    // 获取连接
                    connection = HBaseUtil.getConnection();
                }

                @Override
                public void close() throws Exception {
                    // 关闭连接
                    HBaseUtil.closeConnection(connection);
                }

                @Override
                public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                    // 使用读取的配置表数据  到HBase中创建与之对应的表格
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String op = jsonObject.getString("op");
                        TableProcessDim dim;
                        if ("d".equals(op)) {
                            dim = jsonObject.getObject("before", TableProcessDim.class);
                            // 当配置表发送一个D类型的数据  对应HBase需要删除一张维度表
                            deleteTable(dim);

                        } else if ("c".equals(op) || "r".equals(op)) {
                            dim = jsonObject.getObject("after", TableProcessDim.class);
                            createTable(dim);

                        } else {
                            dim = jsonObject.getObject("after", TableProcessDim.class);
                            deleteTable(dim);
                            createTable(dim);
                        }
                        dim.setOp(op);
                        out.collect(dim);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                private void createTable(TableProcessDim dim) {
                    String sinkFamily = dim.getSinkFamily();
                    String[] split = sinkFamily.split(",");
                    try {
                        HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                private void deleteTable(TableProcessDim dim) {
                    try {
                        HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap((FlatMapFunction<String, JSONObject>) (value, out) -> {
            try {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String database = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                JSONObject data = jsonObject.getJSONObject("data");
                if ("gmall".equals(database) &&
                        !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)
                        && data != null && data.size() != 0) {
                    out.collect(jsonObject);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).returns(JSONObject.class);
    }
}
