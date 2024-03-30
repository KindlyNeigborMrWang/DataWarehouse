package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.app.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author yhm
 * @create 2023-12-24 9:25
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4,"dwd_trade_order_pay_suc_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15L));
        // 核心业务逻辑
        // 1. 读取topic_db主题数据
        createTopicDb(groupId,tableEnv);

        // 2. 筛选出支付成功的数据
        filterPaymentTable(tableEnv);

        // 3. 读取下单详情表数据
        createDWDOrderDetail(tableEnv, groupId);

        // 4. 创建base_dic字典表
        createBaseDic(tableEnv);

        // 5. 使用interval join完成支付成功流和订单详情流数据关联
        intervalJoin(tableEnv);

        // 6. 使用lookup join完成维度退化
        Table resultTable = lookupJoin(tableEnv);

        // 7. 创建upsert kafka写出
        createUpsertKafkaSink(tableEnv);

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();

    }

    public void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(\n" +
                "  id STRING,\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  payment_type_code STRING,\n" +
                "  payment_type_name STRING,\n" +
                "  payment_time STRING,\n" +
                "  sku_id STRING,\n" +
                "  province_id STRING,\n" +
                "  activity_id STRING,\n" +
                "  activity_rule_id STRING,\n" +
                "  coupon_id STRING,\n" +
                "  sku_name STRING,\n" +
                "  order_price STRING,\n" +
                "  sku_num STRING,\n" +
                "  split_total_amount STRING,\n" +
                "  split_activity_amount STRING,\n" +
                "  split_coupon_amount STRING,\n" +
                "  ts bigint ,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    public Table lookupJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT \n" +
                    "  id,\n" +
                    "  order_id,\n" +
                    "  user_id,\n" +
                    "  payment_type payment_type_code,\n" +
                    "  info.dic_name payment_type_name,\n" +
                    "  payment_time,\n" +
                    "  sku_id,\n" +
                    "  province_id,\n" +
                    "  activity_id,\n" +
                    "  activity_rule_id,\n" +
                    "  coupon_id,\n" +
                    "  sku_name,\n" +
                    "  order_price,\n" +
                    "  sku_num,\n" +
                    "  split_total_amount,\n" +
                    "  split_activity_amount,\n" +
                    "  split_coupon_amount,\n" +
                    "  ts\n" +
                    "FROM pay_order p\n" +
                    "left join base_dic  FOR SYSTEM_TIME AS OF p.proc_time  as b\n" +
                    "on p.payment_type = b.rowkey");
    }

    public void intervalJoin(StreamTableEnvironment tableEnv) {
        Table payOrderTable = tableEnv.sqlQuery("SELECT \n" +
                "  od.id,\n" +
                "  p.order_id,\n" +
                "  p.user_id,\n" +
                "  payment_type,\n" +
                "  callback_time payment_time,\n" +
                "  sku_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  p.ts,\n" +
                "  p.proc_time\n" +
                "FROM payment p, order_detail od\n" +
                "WHERE p.order_id = od.order_id\n" +
                "AND p.row_time BETWEEN od.row_time - INTERVAL '15' MINUTE AND od.row_time + INTERVAL '5' SECOND");

        tableEnv.createTemporaryView("pay_order",payOrderTable);
    }

    public void createDWDOrderDetail(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table order_detail (\n" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  create_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint,\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts * 1000,3), \n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,groupId));
    }

    public void filterPaymentTable(StreamTableEnvironment tableEnv) {
        Table paymentTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id,\n" +
                "  `data`['order_id'] order_id,\n" +
                "  `data`['user_id'] user_id,\n" +
                "  `data`['payment_type'] payment_type,\n" +
                "  `data`['total_amount'] total_amount,\n" +
                "  `data`['callback_time'] callback_time,\n" +
                "   ts,\n" +
                "   row_time,\n" +
                "   proc_time\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='payment_info'\n" +
                "and `type`='update'\n" +
                "and `old`['payment_status'] is not null\n" +
                "and `data`['payment_status'] = '1602'\n" +
                "");
        tableEnv.createTemporaryView("payment",paymentTable);
    }
}
