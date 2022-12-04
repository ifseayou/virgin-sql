-- Author --> benchen
-- Target --> test_app.live_goods_category_detail_df
-- Date   --> 2022/12/04
-- Desc   -->
/**
  Vectorized query execution  greatly reduces the CPU usage for typical query operations like scans, filters, aggregates, and joins
  A standard query execution system processes one row at a time. This involves long code paths and significant metadata interpretation
  in the inner loop of execution. Vectorized query execution streamlines operations by processing a block of 1024 rows at a time. Within the block,
  each column is stored as a vector (an array of a primitive data type). Simple operations like arithmetic and comparisons are done by quickly iterating
  through the vectors in a tight loop, with no or very few function calls or conditional branches inside the loop.
  These loops compile in a streamlined way that uses relatively few instructions and finishes each instruction in fewer clock cycles,
  on average, by effectively using the processor pipeline and cache memory. A detailed design document is attached to the vectorized query execution JIRA,
  at https://issues.apache.org/jira/browse/HIVE-4160.


  set hive.vectorized.execution.enabled=false;在 union all 和 distinct 出现的时候，需要设置为 false
 */

create table if not exists test_app.live_goods_category_detail_df
(
    date_id          string comment '下单日期',
    brand_name       string comment '品牌',
    goods_category1  string comment '一级类目',
    goods_category2  string comment '二级类目',
    goods_category3  string comment '三级类目',
    goods_category4  string comment '四级类目',
    order_from_name  string comment '平台',
    order_num        int comment '付款订单量',
    deal_order_num   int comment '成交订单量',
    goods_num        int comment '销量',
    deal_goods_num   int comment '成交商品量',
    sales_amt        decimal(17, 2) comment 'GMV',
    pay_sales_amt    decimal(17, 2) comment '付款GMV',
    deal_sales_amt   decimal(17, 2) comment '实际GMV',
    refund_amt       decimal(17, 2) comment '退款金额',
    refund_order_num int comment '退款订单量',
    etl_time         string comment '处理时间'
)
    comment '类目统计表'
    stored as rcfile
;


set spark.app.name=test_app.live_goods_category_detail_df;
set hive.vectorized.execution.enabled=false;

insert overwrite table test_app.live_goods_category_detail_df
select cast(date_id as string)                                                                  as date_id
     , brand_name
     , goods_category1
     , goods_category2
     , goods_category3
     , goods_category4
     , order_from_name
     , count(distinct case when order_status not in ('0', '4') then platform_order_no end)      as order_num        -- 付款订单量
     , count(distinct case when order_status not in ('0', '4', '7') then platform_order_no end) as deal_order_num   -- 成交订单量
     , nvl(sum(case when order_status not in ('0', '4') then nvl(goods_num, 0) end), 0)         as goods_num        -- 付款商品量
     , nvl(sum(case when order_status not in ('0', '4', '7') then nvl(goods_num, 0) end), 0)    as deal_goods_num   -- 成交商品量
     , nvl(sum(nvl(sale_amt, 0)), 0)                                                            as sales_amt        -- 下单gmv
     , nvl(sum(case when order_status not in ('0', '4') then nvl(sale_amt, 0) end), 0)          as pay_sales_amt    -- 付款gmv
     , nvl(sum(case when order_status not in ('0', '4', '7') then nvl(sale_amt, 0) end), 0)     as deal_sales_amt   -- 实际gmv
     , nvl(sum(case when order_status = '7' then nvl(sale_amt, 0) else 0 end), 0)               as refund_amt       -- 退款金额
     , count(distinct case when order_status = '7' then platform_order_no end)                  as refund_order_num -- 退款订单量
     , from_unixtime(unix_timestamp(), 'yyyy-mm-dd hh:mm:ss')                                   as etl_time         --处理时间
from (select to_date(order_created_at)                                 as date_id         --日期
           , platform_order_no                                                            --订单号
           , order_status                                                                 --订单状态
           , if(brand_name is null, '', brand_name)                    as brand_name      --品牌
           , if(cate1_name is null, '', cate1_name)                    as goods_category1 --一级分类
           , if(cate2_name is null, '', cate2_name)                    as goods_category2 --二级分类
           , if(cate3_name is null, '', cate3_name)                    as goods_category3 --三级分类
           , if(cate4_name is null, '', cate4_name)                    as goods_category4 --四级分类
           , goods_num                                                                    --商品数量
           , deal_amt                                                  as sale_amt        --成交价
           , if(order_from_name = '抖音', '抖音小店', order_from_name) as order_from_name --平台
      from dw.live_order_goods_extend_df
      where  order_created_at >= '${D-T_-180}'
) t
group by date_id
       , brand_name
       , goods_category1
       , goods_category2
       , goods_category3
       , goods_category4
       , order_from_name
union all
select date_id
     , brand_name
     , goods_category1
     , goods_category2
     , goods_category3
     , goods_category4
     , order_from_name
     , order_num
     , deal_order_num
     , goods_num
     , deal_goods_num
     , sales_amt
     , pay_sales_amt
     , deal_sales_amt
     , refund_amt
     , refund_order_num
     , etl_time
from app.live_goods_category_detail_df
where date_id < '${D-T_-180}'