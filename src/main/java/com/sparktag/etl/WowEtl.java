package com.sparktag.etl;

import com.alibaba.fastjson.JSON;
import com.sparktag.util.SparkUtils;
import com.sparktag.util.date.DateStyle;
import com.sparktag.util.date.DateUtil;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class WowEtl {

    // 每周环比注册数 reg count wow (week on week/  M O M)
    public static List<Reg> regCount(SparkSession session) {

        ZoneId zoneId = ZoneId.systemDefault();                                     // 当前时区
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);     // 起始时间
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());         //
        Date nowDayOne = DateUtil.addDay(nowDaySeven, -7);
        Date lastDaySeven = DateUtil.addDay(nowDayOne, -7);

        // lastDaySeven nowDaySeven

        // date_format(日期字段,'yyyy-MM-dd')
        String sql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(id) as regCount from i_member.t_member where create_time >='%s' " +
                " and create_time < '%s' group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql, DateUtil.DateToString(lastDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS),
                DateUtil.DateToString(nowDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<Reg> collect = list.stream().map(str -> JSON.parseObject(str, Reg.class)).collect(Collectors.toList());
        return collect;
    }

    // 每周环比订单数 order count wow
    public static List<Order> orderCount(SparkSession session) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());
        Date nowDayOne = DateUtil.addDay(nowDaySeven, -7);
        Date lastDaySeven = DateUtil.addDay(nowDayOne, -7);
        // i_order.t_order

        String sql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(order_id) as orderCount from i_order.t_order where create_time >='%s' and create_time < '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql, DateUtil.DateToString(lastDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS),
                DateUtil.DateToString(nowDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);

        List<String> list = dataset.toJSON().collectAsList();
        List<Order> collect = list.stream().map(str -> JSON.parseObject(str, Order.class)).collect(Collectors.toList());
        return collect;

    }


    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        List<Reg> regs = regCount(session);
        List<Order> orders = orderCount(session);
        System.out.println("======" + regs);
        System.out.println("======" + orders);

    }


    @Data
    // 每天注册数据统计
    static class Reg {
        private String day;
        private Integer regCount;

        public void setDay(String day) {
            this.day = day;
        }

        public void setRegCount(Integer regCount) {
            this.regCount = regCount;
        }
    }

    @Data
    // 每天订单数据统计
    static class Order {
        private String day;
        private Integer orderCount;

        public void setDay(String day) {
            this.day = day;
        }

        public void setOrderCount(Integer orderCount) {
            this.orderCount = orderCount;
        }
    }

}
