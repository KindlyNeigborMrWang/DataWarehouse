package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.app.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @author yhm
 * @create 2023-12-27 14:09
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022,4,"dws_traffic_vc_ch_ar_is_new_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务
        // 1. 读取DWD的page主题数据
//        stream.print();

        // 2. 进行清洗过滤 转换结构为jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 按照mid进行分组  判断独立访客
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = mapUvBean(jsonObjStream);

//        beanStream.print();
        // 4. 添加水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream = withWaterMark(beanStream);

        // 5. 按照粒度分组
        KeyedStream<TrafficPageViewBean, String> keyedStream = keyByVcChArIsNew(withWaterMarkStream);

        // 6. 开窗
        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // 7. 集合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceFullStream = getReduce(windowStream);

//        reduceFullStream.print();

        // 8. 写出到doris
        reduceFullStream.map(new DorisMapFunction<TrafficPageViewBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));

    }

    public SingleOutputStreamOperator<TrafficPageViewBean> getReduce(WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream) {
        return windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                // 将多个元素的度量值累加到一起
                // v1 表示  累加的结果 (第一次调用第一个元素)
                // v2 表示  累加进来的新元素
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDate(window.getStart());
                String edt = DateFormatUtil.tsToDate(window.getEnd());
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (TrafficPageViewBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCur_date(curDt);
                    out.collect(element);
                }
            }
        });
    }

    public KeyedStream<TrafficPageViewBean, String> keyByVcChArIsNew(SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean value) throws Exception {
                return value.getVc() + ":" + value.getCh() + ":" + value.getAr() + ":" + value.getIsNew();
            }
        });
    }

    public SingleOutputStreamOperator<TrafficPageViewBean> withWaterMark(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
        return beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));
    }

    public SingleOutputStreamOperator<TrafficPageViewBean> mapUvBean(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);

                // 设置状态的存活时间
                lastLoginDtDesc.enableTimeToLive(StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDesc);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                // 判断独立访客
                // 当前数据的日期
                Long ts = value.getLong("ts");
                JSONObject common = value.getJSONObject("common");
                JSONObject page = value.getJSONObject("page");
                String curDt = DateFormatUtil.tsToDate(ts);
                String lastLoginDt = lastLoginDtState.value();
                Long uvCt = 0L;
                Long svCt = 0L;
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    // 状态没有存日期或者状态的日期和数据的日期不是同一天
                    // 当前是一条独立访客
                    uvCt = 1L;
                    // 是独立访客 还要更新一个状态
                    lastLoginDtState.update(curDt);
                }
                // 判断会话数的方法
                // 判断last_page_id == null  新会话的开始

                String lastPageId = page.getString("last_page_id");
                if (lastPageId == null) {
                    svCt = 1L;
                }

                out.collect(TrafficPageViewBean.builder()
                        .vc(common.getString("vc"))
                        .ar(common.getString("ar"))
                        .ch(common.getString("ch"))
                        .isNew(common.getString("is_new"))
                        .uvCt(uvCt)
                        .svCt(svCt)
                        .pvCt(1L)
                        .durSum(page.getLong("during_time"))
                        .sid(common.getString("sid"))
                        .ts(ts)
                        .build());
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    Long ts = jsonObject.getLong("ts");
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    if (mid != null && ts != null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据" + value);
                }
            }
        });
    }
}
