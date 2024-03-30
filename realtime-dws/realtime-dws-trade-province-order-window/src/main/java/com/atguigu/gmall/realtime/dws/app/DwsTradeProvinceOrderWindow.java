package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.app.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2024-01-03 14:31
 */
public class DwsTradeProvinceOrderWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10030,4,"dws_trade_province_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心逻辑
        // 1. 读取数据DWD主题下单明细数据
//        stream.print();

        // 2. 清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = withWaterMark(jsonObjStream);

        // 4. 度量值去重 转换为javaBean
        KeyedStream<JSONObject, String> keyedStream = getKeyById(withWaterMarkStream);

        SingleOutputStreamOperator<TradeProvinceOrderBean> beanStream = getOrderAmountBean(keyedStream);


        // 5. 分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream = windowAndAgg(beanStream);
//        reduceStream.print();

        // 6. 补全维度信息
        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceNameStream = getDimJoin(reduceStream);
//        provinceNameStream.print();

        // 7. 写出到doris
        provinceNameStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW));

    }

    public SingleOutputStreamOperator<TradeProvinceOrderBean> getDimJoin(SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream) {
        return AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeProvinceOrderBean>() {
            @Override
            public String getId(TradeProvinceOrderBean input) {
                return input.getProvinceId();
            }

            @Override
            public String getTableName() {
                return "dim_base_province";
            }

            @Override
            public void join(TradeProvinceOrderBean input, JSONObject dim) {
                input.setProvinceName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);
    }

    public SingleOutputStreamOperator<TradeProvinceOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeProvinceOrderBean> beanStream) {
        return beanStream.keyBy(new KeySelector<TradeProvinceOrderBean, String>() {
            @Override
            public String getKey(TradeProvinceOrderBean value) throws Exception {
                return value.getProvinceId();
            }
        }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TradeProvinceOrderBean> elements, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeProvinceOrderBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            element.setOrderCount((long) element.getOrderIdSet().size());
                            out.collect(element);
                        }
                    }
                });
    }

    public SingleOutputStreamOperator<TradeProvinceOrderBean> getOrderAmountBean(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.map(new RichMapFunction<JSONObject, TradeProvinceOrderBean>() {
            ValueState<BigDecimal> lastTotalAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigDecimal> lastTotalAmountDesc = new ValueStateDescriptor<>("last_total_amount", BigDecimal.class);
                lastTotalAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastTotalAmountState = getRuntimeContext().getState(lastTotalAmountDesc);
            }

            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {

                HashSet<String> hashSet = new HashSet<>();
                hashSet.add(jsonObject.getString("order_id"));
                BigDecimal lastTotalAmount = lastTotalAmountState.value();
                lastTotalAmount = lastTotalAmount == null ? new BigDecimal("0") : lastTotalAmount;
                BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                lastTotalAmountState.update(splitTotalAmount);

                return TradeProvinceOrderBean.builder()
                        .orderIdSet(hashSet)
                        .provinceId(jsonObject.getString("province_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .orderAmount(splitTotalAmount.subtract(lastTotalAmount))
                        .build();
            }
        });
    }

    public KeyedStream<JSONObject, String> getKeyById(SingleOutputStreamOperator<JSONObject> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> withWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    if (value != null) {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String id = jsonObject.getString("id");
                        String orderId = jsonObject.getString("order_id");
                        String provinceId = jsonObject.getString("province_id");
                        Long ts = jsonObject.getLong("ts");

                        if (id != null && orderId != null && provinceId != null && ts != null) {
                            jsonObject.put("ts", ts * 1000);
                            out.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据" + value);
                }
            }
        });
    }
}
