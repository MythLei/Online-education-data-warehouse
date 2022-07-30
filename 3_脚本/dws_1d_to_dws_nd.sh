#!/bin/bash
APP=edu

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

#交易域用户课程粒度订单最近n日汇总事实表
dws_trade_user_course_order_nd="
insert overwrite table ${APP}.dws_trade_user_course_order_nd partition(dt)
select
    user_id,
    course_id,
    course_name,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_user_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_user_num_1d),
    sum(order_total_amount_1d),
	dt
from ${APP}.dws_trade_user_course_order_1d
where dt>=date_add('$do_date',-29)
group by  dt,user_id , course_id , course_name;
"
#交易域用户粒度订单最近n日汇总事实表
dws_trade_user_order_nd="
insert overwrite table ${APP}.dws_trade_user_order_nd partition(dt)
select
    user_id,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_total_amount_1d),
	dt
from ${APP}.dws_trade_user_order_1d
where dt>=date_add('$do_date',-29)
group by user_id,dt;
"
#考试域用户试卷粒度最近n日汇总表
dws_exam_user_paper_exam_nd="
insert overwrite table ${APP}.dws_exam_user_paper_exam_nd partition(dt)
select
    user_id,
    paper_id,
    paper_title,
    course_id,
    course_name,
    score_7d,
    duration_sec_7d,
    score_30d,
    duration_sec_30d,
    dt
    from(
            select
                user_id,
                paper_id,
                paper_title,
                course_id,
                course_name,
                sum(if(dt>=date_sub('$do_date', 6), score_1d, 0)) score_7d,
                sum(if(dt>date_sub('$do_date', 6), duration_sec_1d, 0)) duration_sec_7d,
                sum(score_1d) score_30d,
                sum(duration_sec_1d) duration_sec_30d,
                dt
            from ${APP}.dws_exam_user_paper_exam_1d
            where dt>=date_sub('$do_date', 29)
            group by user_id,paper_id,paper_title,course_id,course_name,dt
        )t;"
#交易域用户粒度订单最近n日汇总事实表

dws_trade_user_order_nd="
insert overwrite table ${APP}.dws_trade_user_order_nd partition(dt)
select
    user_id,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_total_amount_1d),
	dt
from ${APP}.dws_trade_user_order_1d
where dt>=date_add('$do_date',-29)
group by user_id,dt;
"
#播放域用户视频粒度观看N日汇总表
dws_user_video_play_nd="
insert overwrite table ${APP}.dws_user_video_play_nd partition(dt)
select
    user_id,
    course_id,
    course_name,
    chapter_id,
    chapter_name,
    video_id,
    sum(if(dt>=date_sub('$do_date',6),during_video_time_1d,0)),
    sum(if(dt>=date_sub('$do_date',6),view_video_count_1d,0)),
    max(view_video_progress_1d),
    sum(if(dt>=date_sub('$do_date',29),during_video_time_1d,0)),
    sum(if(dt>=date_sub('$do_date',29),view_video_count_1d,0)),
    max(view_video_progress_1d),
	dt
from ${APP}.dws_user_video_play_1d
where dt>=date_sub('$do_date',29)
group by user_id,course_id,course_name,chapter_id,chapter_name,video_id,dt;
"
case $1 in
    "dws_trade_user_course_order_nd" )
        hive -e "$dws_trade_user_course_order_nd"
    ;;
    "dws_trade_user_order_nd" )
        hive -e "$dws_trade_user_order_nd"
    ;;
	 "dws_exam_user_paper_exam_nd" )
        hive -e "$dws_exam_user_paper_exam_nd"
    ;;
	"dws_trade_user_order_nd" )
        hive -e "$dws_trade_user_order_nd"
    ;;
	"dws_user_video_play_nd" )
        hive -e "$dws_user_video_play_nd"
    ;;
    "all" )
        hive -e "$dws_trade_user_course_order_nd$dws_trade_user_order_nd""$dws_exam_user_paper_exam_nd""$dws_trade_user_order_nd""$dws_user_video_play_nd"
    ;;
esac
