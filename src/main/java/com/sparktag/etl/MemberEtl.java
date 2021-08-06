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

        MemberVo vo = new MemberVo();
        vo.setMemberSexes(memberSexes);
        vo.setMemberChannels(memberChannels);
        vo.setMemberMpSubs(memberMpSubs);

        System.out.println("===========" + JSON.toJSONString(vo));
    }

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
        private List<MemberSex> memberSexes;
        private List<MemberChannel> memberChannels;
        private List<MemberMpSub> memberMpSubs;

        public void setMemberSexes(List<MemberSex> memberSexes) {
            this.memberSexes = memberSexes;
        }

        public void setMemberChannels(List<MemberChannel> memberChannels) {
            this.memberChannels = memberChannels;
        }

        public void setMemberMpSubs(List<MemberMpSub> memberMpSubs) {
            this.memberMpSubs = memberMpSubs;
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

}

