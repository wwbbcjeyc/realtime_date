package com.zjtd.realtime.bean;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:10
 * @Version 1.0
 */
import com.zjtd.realtime.common.TransientSink;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class OrderWide {


    Long detail_id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;


    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;


    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;

    BigDecimal original_total_amount;

    BigDecimal feight_fee;


    BigDecimal split_feight_fee;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;

    BigDecimal split_total_amount;



    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;

    @TransientSink
    BigDecimal sku_cost_amount;

//    List<OrderDetailActivity> orderDetailActivityList =new ArrayList<>();
//
//    List<OrderDetailCoupon> orderDetailCouponList  =new ArrayList<>();;

    @TransientSink
    OrderDetailActivity order_detail_activity  =null;
    //
    @TransientSink
    OrderDetailCoupon order_detail_coupon   =null;
//    String if_first_order;//查询状态得到
//
//    String province_name;//查询维表得到
//    String province_area_code;
//    String province_3166_2_code;
//
//    String user_age_group;
//    String user_gender;





//    Long spu_id;     //作为维度数据 要关联进来
//    Long tm_id;
//    Long category3_id;
//    String spu_name;
//    String tm_name;
//    String category3_name;


    public OrderWide(OrderInfo orderInfo,OrderDetail orderDetail){
        mergeOrderInfo(orderInfo);
        mergeOrderDetail(orderDetail);

    }

    public OrderWide(OrderDetailCoupon orderDetailCoupon){
        this.order_id=orderDetailCoupon.getOrder_id();
        this.detail_id=orderDetailCoupon.getOrder_detail_id();
        this.order_detail_coupon=orderDetailCoupon;
        //   this.orderDetailCouponList.add(orderDetailCoupon) ;

    }

    public OrderWide(OrderDetailActivity orderDetailActivity){
        this.order_id=orderDetailActivity.getOrder_id();
        this.detail_id=orderDetailActivity.getOrder_detail_id();
        this.order_detail_activity=orderDetailActivity;
        //   this.orderDetailActivityList.add(orderDetailActivity) ;

    }




    public void  mergeOrderInfo(OrderInfo orderInfo  )  {
        if (orderInfo != null) {
            this.order_id = orderInfo.id;
            this.order_status = orderInfo.order_status;
            this.create_time = orderInfo.create_time;
            this.create_date = orderInfo.create_date;

            this.activity_reduce_amount = orderInfo.activity_reduce_amount;
            this.coupon_reduce_amount = orderInfo.coupon_reduce_amount;
            this.original_total_amount = orderInfo.original_total_amount;
            this.feight_fee = orderInfo.feight_fee;
            this.total_amount =  orderInfo.total_amount;

            this.province_id = orderInfo.province_id;
            this.user_id = orderInfo.user_id;
        }
    }

    public void mergeOrderDetail(OrderDetail orderDetail  )  {
        if (orderDetail != null) {
            this.detail_id = orderDetail.id;
            this.sku_id = orderDetail.sku_id;
            this.sku_name = orderDetail.sku_name;
            this.order_price = orderDetail.order_price;
            this.sku_num = orderDetail.sku_num;

            this.split_activity_amount=orderDetail.split_activity_amount;
            this.split_coupon_amount=orderDetail.split_coupon_amount;
            this.split_total_amount=orderDetail.split_total_amount;

        }
    }

    public void mergeOtherOrderWide(OrderWide otherOrderWide){

        this.order_status = ObjectUtils.firstNonNull( this.order_status ,otherOrderWide.order_status);
        this.create_time =  ObjectUtils.firstNonNull(this.create_time,otherOrderWide.create_time);
        this.create_date =  ObjectUtils.firstNonNull(this.create_date,otherOrderWide.create_date);

        this.coupon_reduce_amount =  ObjectUtils.firstNonNull(this.coupon_reduce_amount,otherOrderWide.coupon_reduce_amount);
        this.activity_reduce_amount =  ObjectUtils.firstNonNull(this.activity_reduce_amount,otherOrderWide.activity_reduce_amount);
        this.original_total_amount =  ObjectUtils.firstNonNull(this.original_total_amount,otherOrderWide.original_total_amount);
        this.feight_fee = ObjectUtils.firstNonNull( this.feight_fee,otherOrderWide.feight_fee);
        this.total_amount =  ObjectUtils.firstNonNull( this.total_amount,otherOrderWide.total_amount);


        this.user_id =  ObjectUtils.<Long>firstNonNull(this.user_id,otherOrderWide.user_id);
        this.sku_id = ObjectUtils.firstNonNull( this.sku_id,otherOrderWide.sku_id);
        this.sku_name =  ObjectUtils.firstNonNull(this.sku_name,otherOrderWide.sku_name);
        this.order_price =  ObjectUtils.firstNonNull(this.order_price,otherOrderWide.order_price);
        this.sku_num = ObjectUtils.firstNonNull( this.sku_num,otherOrderWide.sku_num);

        this.split_activity_amount=ObjectUtils.firstNonNull(this.split_activity_amount);
        this.split_coupon_amount=ObjectUtils.firstNonNull(this.split_coupon_amount);
        this.split_total_amount=ObjectUtils.firstNonNull(this.split_total_amount);

        this.order_detail_activity =ObjectUtils.firstNonNull( this.order_detail_activity,otherOrderWide.order_detail_activity);
        this.order_detail_coupon =ObjectUtils.firstNonNull( this.order_detail_coupon,otherOrderWide.order_detail_coupon);
//        this.orderDetailActivityList.addAll(otherOrderWide.getOrderDetailActivityList());
//        this.orderDetailCouponList.addAll(otherOrderWide.getOrderDetailCouponList());

    }

}
