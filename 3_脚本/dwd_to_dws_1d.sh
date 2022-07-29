#!/bin/bash
APP=edu

# 如果输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

#交易域用户课程粒度订单最近1日汇总事实表
dws_trade_user_course_order_1d="
insert overwrite table ${APP}.dws_trade_user_course_order_1d partition(dt='$do_date')
select
    user_id,
    course_id ,
    course_name ,
    order_count_1d ,
    order_user_num_1d ,
    order_total_amount_1d
from
    (
        select
            dt,
            user_id,
            course_id ,
            count(*)  order_count_1d ,
            count(distinct(user_id)) order_user_num_1d ,
            sum(origin_amount) order_total_amount_1d
        from ${APP}.dwd_trade_order_detail_inc
        GROUP BY dt,user_id,course_id
    )od
        left join
    (
        select
            id,
            course_name
        from ${APP}.dim_course_info_full
        GROUP BY id,course_name
    )cour
    on od.course_id=cour.id;
"
#交易域用户粒度订单最近1日汇总事实表
dws_trade_user_order_1d="
insert overwrite table ${APP}.dws_trade_user_order_1d partition(dt='$do_date')
select
    user_id,
    count(distinct(order_id)),
    sum(origin_amount)
from ${APP}.dwd_trade_order_detail_inc
group by user_id,dt;
"

#考试域用户试卷粒度最近1日汇总表
dws_exam_user_paper_exam_1d="
insert overwrite table ${APP}.dws_exam_user_paper_exam_1d partition(dt='$do_date')
select
    epe.user_id,
    epe.paper_id,
    tp.paper_title,
    tp.course_id,
    ci.course_name,
    epe.score_1d,
    epe.duration_sec_1d
from
    (
        select
            dt,
            user_id,
            paper_id,
            sum(score) score_1d,
            sum(duration_sec) duration_sec_1d
        from ${APP}.dwd_exam_paper_exam_inc
        where dt='$do_date'
        group by user_id,paper_id,dt
    ) epe
        left join
    (
        select
            id,
            paper_title,
            course_id
        from ${APP}.dim_paper_full
        where dt='$do_date'
    ) tp
    on epe.paper_id=tp.id
        left join
    (
        select
            id,
            course_name
        from ${APP}.dim_course_info_full
        where dt='$do_date'
    ) ci
    on tp.course_id=ci.id;
"
#交易域用户粒度支付最近1日汇总事实表
dws_trade_user_payment_1d="
insert overwrite table ${APP}.dws_trade_user_payment_1d partition(dt='$do_date')
select
    user_id,
    count(distinct(order_id)),
    sum(total_amount)
from ${APP}.dwd_trade_pay_detail_suc_inc
group by user_id,dt;
"
#流量域访客页面粒度页面浏览最近1日汇总事实表
dws_traffic_page_visitor_page_view_1d="
insert overwrite table ${APP}.dws_traffic_page_visitor_page_view_1d partition(dt='$do_date')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by mid_id,brand,model,operate_system,page_id;
"

#交易域用户粒度加购最近1日汇总事实表
dws_trade_user_cart_add_1d="
insert overwrite table ${APP}.dws_trade_user_cart_add_1d partition(dt='$do_date')
select
    user_id,
    count(*)
from ${APP}.dwd_trade_cart_add_inc
group by user_id,dt;
"

#播放域用户视频粒度观看一日汇总表
dws_user_video_play_1d="
insert overwrite table ${APP}.dws_user_video_play_1d partition(dt='$do_date')
select
    t1.user_id,
    t1.course_id,
    t2.course_name,
    t1.chapter_id,
    t3.chapter_name,
    t1.video_id,
    during_video_time_1d,
    view_video_count_1d,
    view_video_progress_1d
from
    (select
         user_id,
         course_id,
         chapter_id,
         video_id,
         sum(play_duration) during_video_time_1d,
         count(*)  view_video_count_1d,
         max(watch_progress) view_video_progress_1d
     from
         ${APP}.dwd_play_video_play_inc
     group by user_id, course_id, chapter_id, video_id) t1
        left join(
        select
            id,
            course_name
        from
            ${APP}.dim_course_info_full
    )t2
                 on t1.course_id=t2.id
        left join (
        select
            id,
            chapter_name
        from
            ${APP}.dim_chapter_full
    ) t3
                  on t1.chapter_id=t3.id;
"
#流量域会话粒度页面浏览最近1日汇总表
dws_traffic_session_page_view_1d="
insert overwrite table ${APP}.dws_traffic_session_page_view_1d partition(dt='$do_date')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    a.source_type,
    sum(b1.final_amount),
    sum(during_time),
    count(*)
from
(
    select
        user_id,
        mid_id,
        brand,
        model,
        operate_system,
        version_code,
        channel,
        source_type,
        during_time
    from ${APP}.dwd_traffic_page_view_inc
    where dt='$do_date'
)a
left join
(
    select
        source_site source_type
    from ${APP}.ods_base_source_full
    where dt='$do_date'
)b
on a.source_type=b.source_type
left join
(
    select
        data.user_id     ,
        data.session_id ,
        data.final_amount   final_amount
    from ${APP}.ods_order_info_inc
    where dt='$do_date'
)b1
on a.user_id=b1.user_id
where session_id is not null
group by session_id,mid_id,brand,model,operate_system,version_code,channel,a.source_type;
"
case $1 in
    "dws_trade_user_course_order_1d" )
        hive -e "$dws_trade_user_course_order_1d"
    ;;
    "dws_trade_user_order_1d" )
        hive -e "$dws_trade_user_order_1d"
    ;;
    "dws_exam_user_paper_exam_1d" )
        hive -e "$dws_exam_user_paper_exam_1d"
    ;;
    "dws_trade_user_payment_1d" )
        hive -e "$dws_trade_user_payment_1d"
    ;;
    "dws_traffic_page_visitor_page_view_1d" )
        hive -e "$dws_traffic_page_visitor_page_view_1d"
    ;;
    "dws_trade_user_cart_add_1d" )
        hive -e "$dws_trade_user_cart_add_1d"
    ;;
    "dws_user_video_play_1d" )
        hive -e "$dws_user_video_play_1d"
    ;;
    "dws_traffic_session_page_view_1d" )
        hive -e "$dws_traffic_session_page_view_1d"
    ;;
    "all" )
        hive -e "$dws_trade_user_course_order_1d$dws_trade_user_order_1d$dws_exam_user_paper_exam_1d$dws_trade_user_payment_1d$dws_traffic_page_visitor_page_view_1d$dws_trade_user_cart_add_1d$dws_user_video_play_1d$dws_traffic_session_page_view_1d"
    ;;
esac