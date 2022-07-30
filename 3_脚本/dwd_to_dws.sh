#!/bin/bash
APP=edu

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
   echo "请传入日期参数"
   exit
fi

# 交易域用户课程粒度订单最近1日汇总事实表首日装载语句
dws_trade_user_course_order_1d="
insert overwrite table ${APP}.dws_trade_user_course_order_1d partition(dt)
select
    user_id,
    course_id ,
    course_name ,
    order_count_1d ,
    order_user_num_1d ,
    order_total_amount_1d,
    dt
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

# 交易域用户课程粒度订单最近n日汇总事实表首日装载语句
dws_trade_user_course_order_nd="
insert overwrite table ${APP}.dws_trade_user_course_order_nd partition(dt)
select
    user_id ,
    course_id ,
    course_name ,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_user_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_user_num_1d),
    sum(order_total_amount_1d),
    dt
from ${APP}.dws_trade_user_course_order_1d
where dt>=date_add('$do_date',-29)
group by  dt,user_id , course_id , course_name ;
"

# 交易域用户粒度订单最近1日汇总表装载
dws_trade_user_order_1d="
insert overwrite table ${APP}.dws_trade_user_order_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(origin_amount),
    dt
from ${APP}.dwd_trade_order_detail_inc
group by user_id,dt;
"

# 交易域用户粒度订单最近n日汇总表装载
dws_trade_user_order_nd="
insert overwrite table ${APP}.dws_trade_user_order_nd partition(dt='$do_date')
select
    user_id,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_total_amount_1d)
from ${APP}.dws_trade_user_order_1d
where dt>=date_add('$do_date',-29)
group by user_id,dt;
"

# 交易域用户粒度订单历史至今汇总表装载
dws_trade_user_order_td="
insert overwrite table ${APP}.dws_trade_user_order_td partition(dt='$do_date')
select
    user_id,
    min(dt) create_time,
    max(dt) create_time,
    sum(order_count_1d) origin_amount,
    sum(order_total_amount_1d) total_amount
from ${APP}.dws_trade_user_order_1d
group by user_id;
"

# 用户域用户粒度登录历史至今汇总表
dws_user_user_login_td="
insert overwrite table ${APP}.dws_user_user_login_td partition(dt='$do_date')
select
    u.id,
    nvl(login_date_last,date_format(create_time,'yyyy-MM-dd')),
    nvl(login_count_td,1)
from
    (
        select
            id,
            create_time
        from ${APP}.dim_user_zip
        where dt='9999-12-31'
    )u
        left join
    (
        select
            user_id,
            max(dt) login_date_last,
            count(*) login_count_td
        from ${APP}.dwd_user_login_inc
        group by user_id
    )l
    on u.id=l.user_id;
    "


# 交易域用户粒度支付最近1日汇总表装载
dws_trade_user_payment_1d="
insert overwrite table ${APP}.dws_trade_user_payment_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(total_amount),
    dt
from ${APP}.dwd_trade_pay_detail_suc_inc
group by user_id,dt;
"


# 交易域用户粒度加购最近1日汇总表装载
dws_trade_user_cart_add_1d="
insert overwrite table ${APP}.dws_trade_user_cart_add_1d partition(dt)
select
    user_id,
    count(*),
    dt
from ${APP}.dwd_trade_cart_add_inc
group by user_id,dt;
"

#播放域用户视频粒度观看一日数据装载
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
         sum(play_sec) during_video_time_1d,
         count(*)  view_video_count_1d,
         max(position_sec) view_video_progress_1d
     from
         ${APP}.dwd_play_video_play_inc
     group by dt,user_id, video_id,course_id,chapter_id) t1
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

# 考试域用户试卷粒度最近1日汇总表每日装载
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

case $1 in
"dws_trade_user_course_order_1d")
    hive -e $dws_trade_user_course_order_1d
;;
"dws_trade_user_course_order_nd")
    hive -e "$dws_trade_user_course_order_nd"
;;
"dws_trade_user_order_1d")
    hive -e "$dws_trade_user_order_1d"
;;
"dws_trade_user_order_nd")
    hive -e "$dws_trade_user_order_nd"
;;
"dws_trade_user_order_td")
    hive -e "$dws_trade_user_order_td"
;;
"dws_user_user_login_td")
    hive -e "$dws_user_user_login_td"
;;
"dws_trade_user_payment_1d")
    hive -e "$dws_trade_user_payment_1d"
;;
"dws_trade_user_cart_add_1d")
    hive -e "$dws_trade_user_cart_add_1d"
;;
"dws_user_video_play_1d")
    hive -e "$dws_user_video_play_1d"
;;
"dws_exam_user_paper_exam_1d")
    hive -e "$dws_exam_user_paper_exam_1d"
;;
"all")
    hive -e "$dws_trade_user_course_order_1d$dws_trade_user_course_order_nd$dws_trade_user_order_1d$dws_trade_user_order_nd$dws_trade_user_order_td$dws_user_user_login_td$dws_trade_user_payment_1d$dws_trade_user_cart_add_1d$dws_user_video_play_1d$dws_exam_user_paper_exam_1d"
;;
esac
