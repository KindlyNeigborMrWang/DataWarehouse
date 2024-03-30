package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.app.BaseAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yhm
 * @create 2023-12-22 9:38
 */
public class DwdBaseLog extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }



    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务处理
        // 1. 进行ETL过滤不完整的数据
//        stream.print();
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 2. 进行新旧访客修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWaterMark(jsonObjStream);

        SingleOutputStreamOperator<JSONObject> isNewFixStream = isNewFix(keyedStream);

//        isNewFixStream.print();

        // 3. 拆分不同类型的用户行为日志
        // 启动日志: 启动信息   报错信息
        // 页面日志: 页面信息   曝光信息   动作信息  报错信息
//        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<String>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, startTag, errorTag, displayTag, actionTag);

        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

//        pageStream.print("page");
//        startStream.print("start");
//        errorStream.print("error");
//        displayStream.print("display");
//        actionStream.print("action");

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));


    }

    public SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> startTag, OutputTag<String> errorTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                // 核心逻辑  根据数据的不同  拆分到不同的侧输出流
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    // 当前存在报错信息
                    ctx.output(errorTag, err.toJSONString());
                    value.remove("err");
                }

                JSONObject page = value.getJSONObject("page");
                JSONObject start = value.getJSONObject("start");
                JSONObject common = value.getJSONObject("common");
                Long ts = value.getLong("ts");
                if (start != null) {
                    // 当前是启动日志
                    // 注意  输出的是value完整的日志信息
                    ctx.output(startTag, value.toJSONString());
                } else if (page != null) {
                    // 当前为页面日志
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null ){
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            ctx.output(displayTag, display.toJSONString());
                        }
                        value.remove("displays");
                    }


                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null){
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            ctx.output(actionTag, action.toJSONString());
                        }
                        value.remove("actions");
                    }


                    // 只保留page信息 写出到主流
                    out.collect(value.toJSONString());

                } else {
                    // 留空
                }

            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建状态
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                // 1. 获取当前数据的is_new字段
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    // 判断当前状态情况
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
                        // 如果状态不为空  日期也不是今天  说明当前数据错误 不是新访客 伪装新访客
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        // 状态为空
                        firstLoginDtState.update(curDt);
                    } else {
                        // 留空
                        // 当前数据是同一天新访客重复登录
                    }
                } else if ("0".equals(isNew)) {
                    // is_new 为 0
                    if (firstLoginDt == null) {
                        // 老用户  flink实时数仓里面还没有记录过这个访客  需要补充访客的信息
                        // 把访客首次登录日期补充一个值  今天以前的任意一天都可以  使用昨天的日期
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        // 留空
                        // 正常情况 不需要修复
                    }
                } else {
                    // 当前数据is_new 不为0 也不为1 是错误数据
                }

                out.collect(value);
            }
        });
    }

    public KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
//       jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//            @Override
//            public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                return element.getLong("ts");
//            }
//        })).keyBy(new KeySelector<JSONObject, String>() {
//            @Override
//            public String getKey(JSONObject value) throws Exception {
//                return value.getJSONObject("common").getString("mid");
//            }
//        });

        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        if (common != null && common.getString("mid") != null && ts != null){
                            out.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.println("过滤掉脏数据" + value);
                }
            }
        });
    }
}
