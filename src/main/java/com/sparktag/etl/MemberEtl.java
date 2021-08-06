package com.sparktag.etl;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {
    public static void main(String[] args) {
        SparkSession session = init();

        List<MemberSex> memberSexes = memberSex(session);               // 性别比例
        List<MemberChannel> memberChannels = memberRegChannel(session); // 注册渠道
        List<MemberMpSub> memberMpSubs = memberMpSub(session);          // 用户是否关注
        MemberHeat MemberHeat = memberHeat(session);                    // 用户是否活跃

        MemberVo vo = new MemberVo();
        vo.setMemberSexes(memberSexes);
        vo.setMemberChannels(memberChannels);
        vo.setMemberMpSubs(memberMpSubs);
        vo.setMemberHeat(MemberHeat);

        System.out.println("===========" + JSON.toJSONString(vo));
    }

    /* ======================= 7.5 性别 / 注册渠道 / 是否关注公众号 V ======================= */

    // 用户热度
    public static MemberHeat memberHeat(SparkSession session) {

        /** 查询指标: reg / complete / order / orderAgain / coupon
         * 数据来源:
         * reg / complete => i_member.t_member
         * order / orderAgain => i_order.t_order
         * coupon => i_marketing.t_coupon_member
         */

        // reg / complete => i_member.t_member
        Dataset<Row> reg_complete = session.sql("select count(if(phone='null', id, null)) as reg," +
                " count(if(phone!='null', id, null)) as complete," +
                " from i_member.t_member");
        // order / orderAgain => i_order.t_order
        Dataset<Row> order_orderAgain = session.sql("select count(if(t.orderCount = 1, t.member_id, null)) as order," +
                " count(if(t.orderCount >= 2, t.member_id, null)) as orderAgain" +
                " from (select count(order_id) as orderCount, member_id from i_order.t_order group by member_id) as t");

        // coupon => i_marketing.t_coupon_member
        Dataset<Row> coupon = session.sql("select count(distinct member_id) as coupon from i_marketing.t_coupon_member");


        // cross join 使数据以 member_id 教程关联 (数据量小还好, 但大数据情况下非常耗时且低效)
        Dataset<Row> result = coupon.crossJoin(reg_complete).crossJoin(order_orderAgain);

        // 数据 转 数组
        List<MemberHeat> collect = result.toJSON().collectAsList().stream()
                .map(str-> JSON.parseObject(str, MemberHeat.class))
                .collect(Collectors.toList());

        return collect.get(0);
    }

    /* ======================= 7.5 性别 / 注册渠道 / 是否关注公众号 V ======================= */

    // 性别聚合 男女比例
    public static List<MemberSex> memberSex(SparkSession session) {

        // 查询 sql member sex etl 按性格归类
        Dataset<Row> dataset = session.sql("select sex as memberSex, count(id) as sexCount from i_member.t_member group by sex");
        List<String> list = dataset.toJSON().collectAsList();
        System.out.println("===========" + JSON.toJSONString(list));

        List<MemberSex> collect = list.stream()
                .map(str-> JSON.parseObject(str, MemberSex.class))
                .collect(Collectors.toList());

        return collect;
    }

    // 用户注册渠道
    public static List<MemberChannel> memberRegChannel(SparkSession session) {
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel, count(*) as channelCount from i_member.t_member group by member_channel");
        List<String> list = dataset.toJSON().collectAsList();
        System.out.println("===========" + JSON.toJSONString(list));

        List<MemberChannel> collect = list.stream()
                .map(str -> JSON.parseObject(str, MemberChannel.class))
                .collect(Collectors.toList());

        return collect;
    }

    // 用户关注公众号
    public static List<MemberMpSub> memberMpSub(SparkSession session) {
        // hive 中没有 NULL. 只用字符串 'null'
        Dataset<Row> dataset = session.sql("select count(if(mp_open_id != 'null', id, null)) as mpSubCount,"
                +" count(if(mp_open_id = 'null', id, null)) as unSubCount"
                +" from i_member.t_member");
        List<String> list = dataset.toJSON().collectAsList();
        System.out.println("===========" + JSON.toJSONString(list));

        List<MemberMpSub> collect = list.stream()
                .map(str -> JSON.parseObject(str, MemberMpSub.class))
                .collect(Collectors.toList());

        return collect;
    }

    // Spark 查询对象工厂
    public static SparkSession init(){
        SparkConf conf = new SparkConf().setAppName("member_etl").setMaster("local[*]")
                .set("dfs.client.use.datanode.hostname", "true");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .appName("member_etl")
                .master("local[*]")
                .enableHiveSupport() // 需要到 hive 查询数据
                .getOrCreate();

        return session;
    }

    /* ======================= 数据传输对象 V ======================= */

    // 用户指标
    @Data
    static class MemberVo {
        private List<MemberSex> memberSexes;        // 用户性别比例
        private List<MemberChannel> memberChannels; // 用户注册渠道
        private List<MemberMpSub> memberMpSubs;     // 用户是否关注
        private MemberHeat MemberHeat;        // 用户活跃度

        public void setMemberSexes(List<MemberSex> memberSexes) {
            this.memberSexes = memberSexes;
        }

        public void setMemberChannels(List<MemberChannel> memberChannels) {
            this.memberChannels = memberChannels;
        }

        public void setMemberMpSubs(List<MemberMpSub> memberMpSubs) {
            this.memberMpSubs = memberMpSubs;
        }

        public void setMemberHeat(MemberEtl.MemberHeat memberHeat) {
            MemberHeat = memberHeat;
        }
    }

    // 用户比例
    @Data
    static class MemberSex {
        private Integer memberSex;
        private Integer sexCount;

        public void setMemberSex(Integer memberSex) {
            this.memberSex = memberSex;
        }

        public void setSexCount(Integer sexCount) {
            this.sexCount = sexCount;
        }
    }

    // 用户注册渠道
    @Data
    static class MemberChannel {
        private Integer memberChannel;
        private Integer channelCount;

        public void setMemberChannel(Integer memberChannel) {
            this.memberChannel = memberChannel;
        }

        public void setChannelCount(Integer channelCount) {
            this.channelCount = channelCount;
        }
    }

    // 用户是否关注
    @Data
    static class MemberMpSub {
        private Integer mpSubCount;
        private Integer unSubCount;

        public void setMpSubCount(Integer mpSubCount) {
            this.mpSubCount = mpSubCount;
        }

        public void setUnSubCount(Integer unSubCount) {
            this.unSubCount = unSubCount;
        }
    }

    @Data
    // 用户活跃
    static class MemberHeat {
        private Integer reg;
        private Integer complete;
        private Integer order;
        private Integer orderAgain;
        private Integer coupon;

        public void setReg(Integer reg) {
            this.reg = reg;
        }

        public void setComplete(Integer complete) {
            this.complete = complete;
        }

        public void setOrder(Integer order) {
            this.order = order;
        }

        public void setOrderAgain(Integer orderAgain) {
            this.orderAgain = orderAgain;
        }

        public void setCoupon(Integer coupon) {
            this.coupon = coupon;
        }
    }

}

