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
 * @create 2023-12-23 15:04
 */
public class DwdTradeOrderDetail extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024,4,"dwd_trade_order_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        // 在flinkSQL中使用join一定要添加状态的存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        // 核心业务编写
        // 1. 读取topic_db数据
        createTopicDb(groupId,tableEnv);

        // 2. 筛选订单详情表数据
        filterOd(tableEnv);

        // 3. 筛选订单信息表
        filterOi(tableEnv);

        // 4. 筛选订单详情活动关联表
        filterOda(tableEnv);

        // 5. 筛选订单详情优惠券关联表
        filterOdc(tableEnv);

        // 6. 将四张表格join合并
        Table joinTable = getJoinTable(tableEnv);

        // 7. 写出到kafka中
        // 一旦使用了left join 会产生撤回流  此时如果需要将数据写出到kafka
        // 不能使用一般的kafka sink  必须使用upsert kafka
        createUpsertKafkaSink(tableEnv);

        joinTable.insertInto( Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();


    }

    public void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL +" (\n" +
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
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    public Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts \n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on oda.id = od.id\n" +
                "left join order_detail_coupon odc\n" +
                "on odc.id = od.id ");
    }

    public void filterOdc(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type`='insert'");

        tableEnv.createTemporaryView("order_detail_coupon",odcTable);
    }

    public void filterOda(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['activity_id'] activity_id, \n" +
                "  `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity",odaTable);
    }

    public void filterOi(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['user_id'] user_id, \n" +
                "  `data`['province_id'] province_id \n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='insert'");

        tableEnv.createTemporaryView("order_info",oiTable);
    }

    public void filterOd(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['order_id'] order_id, \n" +
                "  `data`['sku_id'] sku_id, \n" +
                "  `data`['sku_name'] sku_name, \n" +
                "  `data`['order_price'] order_price, \n" +
                "  `data`['sku_num'] sku_num, \n" +
                "  `data`['create_time'] create_time, \n" +
                "  `data`['split_total_amount'] split_total_amount, \n" +
                "  `data`['split_activity_amount'] split_activity_amount, \n" +
                "  `data`['split_coupon_amount'] split_coupon_amount, \n" +
                "  ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail",odTable);
    }
}
