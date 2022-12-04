-- Author --> benchen
-- Target --> test_app.fna_compare_zhubo_stat_df
-- Date   --> 2022/11/15
-- Desc   --> test_tmp.app_fna_compare_zhubo_stat_df_02表 存储格式为 orc 的时候，会发生
/**
  Job aborted due to stage failure: Task 0 in stage 6.0 failed 4 times, most recent failure:
  Lost task 0.3 in stage 6.0 (TID 135, cdhnode8, executor 4): UnknownReason
 */
-- 的错误，现在无法知道真正的原因是什么？？但是解决的方法是：
/**
  将test_tmp.app_fna_compare_zhubo_stat_df_02 存储格式更改为 非 ORC格式，比如 rcfile 格式
 */


create table if not exists test_app.fna_compare_zhubo_stat_df
(
    etl_time            timestamp comment 'etl时间',
    order_create_date   string comment '下单日期',
    anchor_nick         string comment '主播昵称',
    order_from_name     string comment '商品平台',
    platform_name       string comment '流量平台',
    cate1_name          string comment '商品1级类目',
    cate2_name          string comment '商品2级类目',
    cate3_name          string comment '商品3级类目',
    cate4_name          string comment '商品4级类目',
    purchaser_dept_name string comment '采销负责人部门',
    brand_name          string comment '品牌',
    pay_amt             decimal(28, 4) comment '付款GMV',
    deal_amt            decimal(28, 4) comment '成交额（实际gmv）',
    cps_deal_amt        decimal(28, 4) comment 'cps成交额（实际gmv）',
    commission          decimal(28, 4) comment '佣金',
    pit_fee             decimal(28, 4) comment '坑位费',
    gross_profit        decimal(28, 4) comment '毛利额',
    refund_amt          decimal(28, 4) comment '退款金额',
    refund_order_num    int comment '退款订单量',
    order_num           int comment '付款订单量'
) comment '商品主播维度今年和去年同期比对'
    row format delimited fields terminated by '\001' lines terminated by '\n'
    stored as rcfile
;

set spark.app.name = test_app.fna_compare_zhubo_stat_df;

set spark.yarn.executor.memoryOverhead = 1024mb;
set yarn.scheduler.maximum-allocation-mb = 20480;

drop table if exists test_tmp.app_fna_compare_zhubo_stat_df_01;
create table if not exists test_tmp.app_fna_compare_zhubo_stat_df_01
    stored as orc as
select order_create_date                                                                                     as order_create_date

     , anchor_nick                                                                                           as anchor_nick
     , order_from_name                                                                                       as order_from_name
     , max(platform_name)                                                                                    as platform_name
     , cate4_name                                                                                            as cate4_name
     , max(cate3_name)                                                                                       as cate3_name
     , max(cate2_name)                                                                                       as cate2_name
     , max(cate1_name)                                                                                       as cate1_name
     , goods_id                                                                                              as goods_id
     , purchaser_dept_name                                                                                   as purchaser_dept_name
     , trim(max(brand_name))                                                                                 as brand_name

     , sum(sales_amt)                                                                                        as sales_amt
     , sum(deal_sales_amt)                                                                                   as deal_sales_amt
     , sum(order_online_commission)                                                                          as order_online_commission
     , max(pit_fee)                                                                                          as pit_fee
     , sum(report_offline_commission)                                                                        as report_offline_commission
     , sum(refund_amt)                                                                                       as refund_amt
     , sum(refund_order_num)                                                                                 as refund_order_num
     , sum(order_num)                                                                                        as order_num
     , sum(if(order_from_name not in ('快手小店', '抖音小店', '有赞微商城', '有赞分销'), deal_sales_amt, 0)) as cps_deal_amt
from app.fna_goods_zhubo_stat_df
group by room_id
       , order_create_date
       , anchor_nick
       , order_from_name
       , cate4_name
       , goods_id
       , purchaser_dept_name
;
drop table if exists test_tmp.app_fna_compare_zhubo_stat_df_02;
create table if not exists test_tmp.app_fna_compare_zhubo_stat_df_02
    stored as orc as
select order_create_date              as order_create_date
     , order_from_name                as order_from_name
     , cate4_name                     as cate4_name
     , purchaser_dept_name            as purchaser_dept_name
     , anchor_nick                    as anchor_nick

     , platform_name                  as platform_name
     , max(cate3_name)                as cate3_name
     , max(cate2_name)                as cate2_name
     , max(cate1_name)                as cate1_name
     , max(brand_name)                as brand_name

     , sum(sales_amt)                 as pay_amt
     , sum(deal_sales_amt)            as deal_amt
     , sum(order_online_commission)   as order_online_commission
     , sum(pit_fee)                   as pit_fee
     , sum(report_offline_commission) as report_offline_commission
     , sum(refund_amt)                as refund_amt
     , sum(refund_order_num)          as refund_order_num
     , sum(order_num)                 as order_num
     , sum(cps_deal_amt)              as cps_deal_amt

from test_tmp.app_fna_compare_zhubo_stat_df_01
group by order_create_date
       , anchor_nick
       , order_from_name
       , platform_name
       , cate4_name
       , purchaser_dept_name
;

insert overwrite table test_app.fna_compare_zhubo_stat_df
select from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , order_create_date                                      as order_create_date
     , anchor_nick                                            as anchor_nick
     , order_from_name                                        as order_from_name
     , platform_name                                          as platform_name
     , cate1_name                                             as cate1_name
     , cate2_name                                             as cate2_name
     , cate3_name                                             as cate3_name
     , cate4_name                                             as cate4_name
     , purchaser_dept_name                                    as purchaser_dept_name
     , brand_name                                             as brand_name

     , pay_amt                                                as pay_amt
     , deal_amt                                               as deal_amt
     , cps_deal_amt                                           as cps_deal_amt

     , nvl(order_online_commission, 0) +
       nvl(report_offline_commission, 0)                      as commission

     , pit_fee                                                as pit_fee
     , nvl(order_online_commission, 0) +
       nvl(report_offline_commission, 0) + nvl(pit_fee, 0)    as gross_profit

     , refund_amt                                             as refund_amt
     , refund_order_num                                       as refund_order_num
     , order_num                                              as order_num
from test_tmp.app_fna_compare_zhubo_stat_df_02
;