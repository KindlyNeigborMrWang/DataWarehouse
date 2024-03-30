package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.app.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2023-12-29 9:34
 */
public class DwsTradeCartAddUuWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(10026,4,"dws_trade_cart_add_uu_window", Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑
        // 1. 读取DWD加购主题数据
//        stream.print();
        // 2. 清洗过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = withWaterMark(jsonObjStream);

        // 4. 按照user_id进行分组
        KeyedStream<JSONObject, String> keyedStream = getKeyByUserId(withWaterMarkStream);

        // 5. 判断是否为独立用户
        SingleOutputStreamOperator<CartAddUuBean> uuCtBeanStream = getUuCt(keyedStream);

        // 6. 开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceStream = windowAndAgg(uuCtBeanStream);

//        reduceStream.print();

        // 7. 写出到doris
        reduceStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_CART_ADD_UU_WINDOW));

    }

    public SingleOutputStreamOperator<CartAddUuBean> windowAndAgg(SingleOutputStreamOperator<CartAddUuBean> uuCtBeanStream) {
        return uuCtBeanStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<CartAddUuBean> elements, Collector<CartAddUuBean> out) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (CartAddUuBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            out.collect(element);
                        }
                    }
                });
    }

    public SingleOutputStreamOperator<CartAddUuBean> getUuCt(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                lastLoginDtDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDesc);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<CartAddUuBean> out) throws Exception {
                // 判断独立用户
                // 比较当前数据的时间和状态中的上次登录的时间
                String curDt = DateFormatUtil.tsToDate(value.getLong("ts"));
                String lastLoginDt = lastLoginDtState.value();
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    // 当前为独立用户
                    lastLoginDtState.update(curDt);
                    out.collect(new CartAddUuBean("", "", "", 1L));
                }
            }
        });
    }

    public KeyedStream<JSONObject, String> getKeyByUserId(SingleOutputStreamOperator<JSONObject> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
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
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String userId = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");
                    if (ts != null && userId != null) {
                        // 将时间戳修改为13位毫秒级
                        jsonObject.put("ts",ts * 1000L);
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据" + value);
                }
            }
        });
    }
}
