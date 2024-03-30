package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.app.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @author yhm
 * @create 2024-01-02 10:22
 */
public class DwsTradeSkuOrderWindowSyncCache extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(10029,4,"dws_trade_sku_order_window_sync_cache", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑
        // 1. 读取DWD层下单主题数据
//        stream.print();

        // 2. 过滤清洗  ->  null
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = withWaterMark(jsonObjStream);

        // 4. 修正度量值 转换数据结构
        KeyedStream<JSONObject, String> keyedStream = getKeyById(withWaterMarkStream);

        SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream = getProcess(keyedStream);

//        processBeanStream.print();

        // 5. 分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = windowAndAgg(processBeanStream);

//        reduceBeanStream.print();

        // 6. 关联维度信息
        // 6.1 关联sku_info  补充维度信息
        reduceBeanStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            Connection connection;
            Jedis jedis;
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getConnection();
                jedis = RedisUtil.getJedis();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(connection);
                RedisUtil.closeJedis(jedis);
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                // 1. 拼接对应的redisKey
                String redisKey = RedisUtil.getRedisKey("dim_sku_info",bean.getSkuId());

                // 2. 读取redis缓存的数据  ->  此处存储的redis的value值是 JSONObject 的字符串
                String dim = jedis.get(redisKey);
                JSONObject dimSkuInfo;

                // 3. 判断redis读取到的数据是否为空
                if (dim == null || dim.length() == 0){
                    // redis没有对应缓存  需要读取HBase
                    System.out.println("没有缓存,读取hbase  " + redisKey);
                    dimSkuInfo = HBaseUtil.getCells(connection,Constant.HBASE_NAMESPACE,"dim_sku_info",bean.getSkuId());
                    // 读取到数据之后  存到redis
                    if (dimSkuInfo.size() != 0){
                        jedis.setex(redisKey,24 * 60 * 60 ,dimSkuInfo.toJSONString());
                    }
                }else {
                    // redis有缓存  直接返回
                    System.out.println("有缓存直接返回redis数据  " + redisKey);
                    dimSkuInfo = JSONObject.parseObject(dim);
                }

                if (dimSkuInfo.size() != 0){
                    // 进行维度关联
                    bean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                    bean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                    bean.setSpuId(dimSkuInfo.getString("spu_id"));
                    bean.setSkuName(dimSkuInfo.getString("sku_name"));
                }else {
                    System.out.println("没有对应的维度信息" + bean);
                }

                return bean;
            }
        }).print();



        // 7. 写出到doris
//        fullDimBeanStream.map(new DorisMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));

    }




    public SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream) {
        return processBeanStream.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
            @Override
            public String getKey(TradeSkuOrderBean value) throws Exception {
                return value.getSkuId();
            }
        })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeSkuOrderBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            out.collect(element);
                        }

                    }
                });
    }

    public SingleOutputStreamOperator<TradeSkuOrderBean> getProcess(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> lastAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountDesc = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastAmountState = getRuntimeContext().getMapState(lastAmountDesc);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                // 调取状态中的度量值
                BigDecimal originalAmount = lastAmountState.get("originalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");

//                if (orderAmount != null){
//                    System.out.println("相同的id " + value);
//                }
                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;

                BigDecimal curOriginalAmount = value.getBigDecimal("order_price").multiply(value.getBigDecimal("sku_num"));
                // 每一条相同id的数据  度量值减去上一条状态中的数据值
                TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                        .skuId(value.getString("sku_id"))
                        .orderDetailId(value.getString("id"))
                        .ts(value.getLong("ts"))
                        .originalAmount(curOriginalAmount.subtract(originalAmount))
                        .orderAmount(value.getBigDecimal("split_total_amount").subtract(orderAmount))
                        .activityReduceAmount(value.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                        .couponReduceAmount(value.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                        .build();

                // 存储当前的度量值
                lastAmountState.put("originalAmount",curOriginalAmount);
                lastAmountState.put("activityReduceAmount",value.getBigDecimal("split_activity_amount"));
                lastAmountState.put("couponReduceAmount",value.getBigDecimal("split_coupon_amount"));
                lastAmountState.put("orderAmount",value.getBigDecimal("split_total_amount"));


                out.collect(bean);
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
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
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
                        Long ts = jsonObject.getLong("ts");
                        String id = jsonObject.getString("id");
                        String skuId = jsonObject.getString("sku_id");
                        if (ts != null && id != null && skuId != null) {
                            jsonObject.put("ts" ,ts * 1000);
                            out.collect(jsonObject);
                        }
                    }
                }catch (Exception e){
                    System.out.println("过滤掉脏数据" + value);
                }
            }
        });
    }
}
