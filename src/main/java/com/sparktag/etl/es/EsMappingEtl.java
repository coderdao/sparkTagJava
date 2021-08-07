package com.sparktag.etl.es;

import java.io.Serializable;
import java.util.List;

public class EsMappingEtl {

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

    }
}
