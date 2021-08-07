package com.sparktag.etl.es;

import com.sparktag.util.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.io.Serializable;
import java.util.List;

public class EsMappingEtl {

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();

        etl(session);
    }

    private static void etl(SparkSession session) {
        Dataset<Row> member = session.sql("select id as memberId, phone, sex, member_channel as channel, mp_open_id as subUpenId," +
                " address_default_id as address, date_format(create_time, 'yyyy-MM-dd') as regTime" +
                " from i_member.t_member");
        Dataset<Row> order_commodity = session.sql( "select o.member as memberId," +
                " date_format(max(o.create_time), 'yyyy-MM-dd ') as orderTime, " +
                " count(o.order_id) as orderCount, " +
                " collect_list(DISTINCT oc.commodity_id) as favGoods, " +
                " sum(o.pay_money) as orderMoney " +
                " from i order.t_order as o left join i_order.t_order_commodity as oc" +
                " on o.order_id = oc.order_id group by o.member_id");

        Dataset<Row> freeCoupon = session.sql( "select member_id as memberId, " +
                " date_format(create_time, 'yyyy-MM-dd ' ) as freeCouponTime " +
                " from i_marketing.t_coupon_member where coupon_id = 1");
        Dataset<Row> couponTimes = session.sql( "select member_id as memberTd," +
                " collect_list(date_format(create_time, ' yyyy-MN-dd ' )) as couponTimes" +
                " from i_marketing.t_coupon_member where coupon_id !=1 group by member_id");
        Dataset<Row> chargeMoney = session.sql("select cm.member_id as memberId , sum(c.coupon_price/2) as chargeMoney " +
                " from i_marketing.t_coupon_member as cm left join i_marketing.t_coupon as c " +
                " on cm.coupon id = c.id where cm.coupon_channel = 1 group by cm.member_id");

        Dataset<Row> overTime = session.sql( "select (to_unix_timestamp(max(arrive_time)) - (to_unix_timestamp(max(pick_time)) " +
                " as overTime, member_id as memberId " +
                " from i_operation.t_delivery group by member_id");
        Dataset<Row> feedback = session.sql( "selec fb.feedback_type as feedback, fb.member_id as memberId" +
                " from i_operation.t_feedback as fb " +
                " left join（select max(id） as mid , member_id as memberId " +
                " from i_operation.t_feedback group by member_id)as t " +
                " on fb.id = t.mid");

        // 每个 dataset 注册一个临时表,
        member.registerTempTable( "member");
        order_commodity.registerTempTable( "oc " );
        freeCoupon.registerTempTable ( "freeCoupon");
        couponTimes.registerTempTable( "couponTimes");
        chargeMoney.registerTempTable("chargeMoney");
        overTime.registerTempTable( "overTime ");
        feedback.registerTempTable( "feedback");

        // 每个临时表进行 left join
        Dataset<Row> result = session.sql( "select m.* , .orderCount , o.orderTime , o.orderWoney,o.favGoods" +
                " fb.freeCouponTime,ct.couponTimes，cm.chargeMoney,ot.overTime,f.feedBack" +
                        " from member as m" +
                        " left ioin oc as o on m.memberId = o.memberId " +
                        " left join freeCoupon as fb on m.memberId = fb.memberId " +
                        " left join couponTimes as ct on m.memberId = ct.memberId " +
                        " left join chargeMoney as cm on m.memberId = cm.memberId " +
                        " left join overTime as ot on m.memberId = ot.memberId " +
                        " left join feedback as f on m.memberId = f.memberId ");
        JavaEsSparkSQL.saveToEs(result, "/tag/_doc");

    }

    // 用户标签
    public static class MemberTag implements Serializable {
        /** i_member.t_member */
        private String memberId;
        private String phone;
        private String sex;
        private String channel;
        private String subOpenId;
        private String address;
        private String regTime ;

        /** i_order */
        private Long orderCount;

        // max(create_time) i_order.t_order
        private String orderTime;
        private Double orderMoney;
        private List<String> favGoods;

        /** i_marketing */
        private String freeCouponTime;
        private List<String> couponTimes;
        private Double chargeMoney;


        private Integer overTime;
        private Integer feedBack;

        public String getMemberId() {
            return memberId;
        }

        public void setMemberId(String memberId) {
            this.memberId = memberId;
        }

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }

        public String getSubOpenId() {
            return subOpenId;
        }

        public void setSubOpenId(String subOpenId) {
            this.subOpenId = subOpenId;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getRegTime() {
            return regTime;
        }

        public void setRegTime(String regTime) {
            this.regTime = regTime;
        }

        public Long getOrderCount() {
            return orderCount;
        }

        public void setOrderCount(Long orderCount) {
            this.orderCount = orderCount;
        }

        public String getOrderTime() {
            return orderTime;
        }

        public void setOrderTime(String orderTime) {
            this.orderTime = orderTime;
        }

        public Double getOrderMoney() {
            return orderMoney;
        }

        public void setOrderMoney(Double orderMoney) {
            this.orderMoney = orderMoney;
        }

        public List<String> getFavGoods() {
            return favGoods;
        }

        public void setFavGoods(List<String> favGoods) {
            this.favGoods = favGoods;
        }

        public String getFreeCouponTime() {
            return freeCouponTime;
        }

        public void setFreeCouponTime(String freeCouponTime) {
            this.freeCouponTime = freeCouponTime;
        }

        public List<String> getCouponTimes() {
            return couponTimes;
        }

        public void setCouponTimes(List<String> couponTimes) {
            this.couponTimes = couponTimes;
        }

        public Double getChargeMoney() {
            return chargeMoney;
        }

        public void setChargeMoney(Double chargeMoney) {
            this.chargeMoney = chargeMoney;
        }

        public Integer getOverTime() {
            return overTime;
        }

        public void setOverTime(Integer overTime) {
            this.overTime = overTime;
        }

        public Integer getFeedBack() {
            return feedBack;
        }

        public void setFeedBack(Integer feedBack) {
            this.feedBack = feedBack;
        }
    }
}
