#!/bin/bash
APP=edu

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi

#互动域课程评价事务事实表
dwd_interaction_review_add_inc="
insert overwrite table ${APP}.dwd_interaction_review_add_inc partition (dt)
select data.id,
       data.user_id,
       data.course_id,
       data.review_stars,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       dt
from ${APP}.ods_review_info_inc
where dt = '$do_date';
"

#交易域章节加购事务事实表
dwd_trade_cart_add_inc="
insert overwrite table ${APP}.dwd_trade_cart_add_inc partition (dt)
select id,
       user_id,
       course_id,
       cart_price,
       session_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       dt
from ${APP}.ods_cart_info_full
where dt = '$do_date';
"

#互动域章节评论事务事实表
dwd_interaction_comment_add_inc="
insert overwrite table ${APP}.dwd_interaction_comment_add_inc partition (dt)
select data.id,
       data.user_id,
       data.chapter_id,
       data.course_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       date_format(data.create_time, 'yyyy-MM-dd')
from ${APP}.ods_comment_info_inc
where dt = '$do_date';
"


#用户域用户登录事务事实表
dwd_user_login_inc="
insert overwrite table ${APP}.dwd_user_login_inc partition (dt)
select user_id
     , date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id
     , date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time
     , channel
     , province_id
     , version_code
     , mid_id
     , brand
     , model
     , operate_system
     , date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')
from (select user_id,
             channel,
             province_id,
             version_code,
             mid_id,
             brand,
             model,
             operate_system,
             ts
      from (select user_id,
                   channel,
                   province_id,
                   version_code,
                   mid_id,
                   brand,
                   model,
                   operate_system,
                   ts,
                   row_number() over (partition by session_id order by ts) rn
            from (select user_id,
                         channel,
                         province_id,
                         version_code,
                         mid_id,
                         brand,
                         model,
                         operate_system,
                         ts,
                         concat(mid_id, '-',
                                last_value(session_start_point, true)
                                           over (partition by mid_id order by ts)) session_id
                  from (select common.uid                              user_id,
                               common.ch                               channel,
                               common.ar                               province_id,
                               common.vc                               version_code,
                               common.mid                              mid_id,
                               common.os                               operate_system,
                               common.md                               model,
                               common.ba                               brand,
                               ts,
                               if(page.last_page_id is null, ts, null) session_start_point
                        from ${APP}.ods_log_inc
                        where dt = '$do_date'
                          and page is not null) t1) t2
            where user_id is not null) t3
      where rn = 1) t4;
"

#用户域用户注册事务事实表
dwd_user_register_inc="
insert overwrite table ${APP}.dwd_user_register_inc partition (dt)
select ui.user_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       date_format(create_time, 'yyyy-MM-dd')

from (select data.id user_id,
             data.create_time
      from ${APP}.ods_user_info_inc
      where dt = '$do_date'
        and type = 'insert') ui
         left join
     (select common.ar  province_id,
             common.uid user_id
      from ${APP}.ods_log_inc
      where dt = '$do_date'
        and common.uid is not null) log
     on ui.user_id = log.user_id;
"
#流量域页面浏览日志表
dwd_traffic_page_view_inc="
insert overwrite table ${APP}.dwd_traffic_page_view_inc partition (dt)
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')                                        date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                               view_time,
       concat(mid_id, '-', last_value(session_start_point, true) over (partition by mid_id order by ts)) session_id,
       during_time,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')                                        date_id


from (select common.ar                               province_id,
             common.ba                               brand,
             common.ch                               channel,
             common.is_new                           is_new,
             common.md                               model,
             common.mid                              mid_id,
             common.os                               operate_system,
             common.uid                              user_id,
             common.vc                               version_code,
             common.sc                               source_type,
             page.during_time,
             page.item                               page_item,
             page.item_type                          page_item_type,
             page.last_page_id,
             page.page_id,
             ts,
             if(page.last_page_id is null, ts, null) session_start_point
      from ${APP}.ods_log_inc
      where dt = '$do_date'
        and page is not null) log;
"
#播放域用户章节观看事务事实表
dwd_play_user_chapter_process_inc="
insert overwrite table ${APP}.dwd_play_user_chapter_process_inc partition (dt)
select data.id,
       data.user_id,
       data.course_id,
       data.chapter_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       data.position_sec,
       date_format(data.create_time, 'yyyy-MM-dd')
from ${APP}.ods_user_chapter_process_inc
where dt = '$do_date';
"
#考试域答题事务事实表
dwd_exam_question_exam_inc="
insert overwrite table ${APP}.dwd_exam_question_exam_inc partition(dt)
select data.id,
       data.exam_id,
       data.paper_id,
       data.question_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       data.is_correct,
       data.score,
	dt
from ${APP}.ods_test_exam_question_inc
where dt = '$do_date';
"
#考试域答卷事务事实表
dwd_exam_paper_exam_inc="
insert overwrite table ${APP}.dwd_exam_paper_exam_inc partition (dt)
select data.id,
       data.user_id,
       data.paper_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       data.submit_time,
       data.score,
       data.duration_sec,
	dt
from ${APP}.ods_test_exam_inc
where dt = '$do_date';
"
#交易域支付成功事务事实表
dwd_trade_pay_detail_suc_inc="
insert overwrite table ${APP}.dwd_trade_pay_detail_suc_inc partition (dt)
select id
     , pay.order_id
     , user_id
     , course_id
     , payment_type_code
     , date_format(callback_time,'yyyy-MM-dd') date_id
     , callback_time
     , total_amount
     , date_format(callback_time,'yyyy-MM-dd') dt
from (
         select data.id,
                data.course_id,
                data.user_id,
                data.order_id,
                data.final_amount total_amount
         from ${APP}.ods_order_detail_inc
         where  dt = '$do_date'
     ) oi
         join
     (
         select data.order_id,
                data.payment_type payment_type_code,
                data.callback_time
         from ${APP}.ods_payment_info_inc
         where  dt = '$do_date'
           and data.payment_type='1102'
     ) pay
     on oi.order_id = pay.order_id;
"
#互动域收藏事务事实表
dwd_interaction_favor_add_inc="
insert overwrite table ${APP}.dwd_interaction_favor_add_inc partition (dt)
select data.id,
       data.course_id,
       data.user_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       dt
from ${APP}.ods_favor_info_inc;
"
#交易域下单明细事务事实表
dwd_trade_order_detail_inc="
insert overwrite table ${APP}.dwd_trade_order_detail_inc partition (dt)
select b.id,
       course_id,
       order_id,
       b.user_id,
       session_id,
       province_id,
       date_id,
       create_time,
       nvl(origin_amount, 0) origin_amount,
       dt
from (
         select data.id,
                data.user_id,
                data.session_id,
                data.province_id
         from ${APP}.ods_order_info_inc
     ) a
         right join (
    select data.id,
           data.course_id,
           data.course_name,
           data.order_id,
           data.user_id,
           data.origin_amount,
           data.coupon_reduce,
           data.final_amount,
           data.create_time,
           date_format(data.create_time, 'yyyy-MM-dd') date_id,
           data.update_time,
           date_format(data.create_time, 'yyyy-MM-dd') dt
    from ${APP}.ods_order_detail_inc
) b
                    on a.user_id = b.user_id;
"




#播放域视频播放事务事实表
dwd_play_video_play_inc="
insert overwrite table ${APP}.dwd_play_video_play_inc partition (dt)
select video_id
     , t1.user_id
     , t1.chapter_id
     , course_id
     , video_time
     , date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd') watch_data
     , watch_progress
     , play_duration
     , dt
from (
         (select video_id
               , user_id
               , course_id
               , chapter_id
               , video_time
               , watch_progress
               , play_duration
               , ts
               , dt
          from (
                   (select dt,
                           id,--视屏编号
                           chapter_id, --章节id
                           course_id, --课程id
                           during_sec video_time --视屏总时长
                    from ${APP}.ods_video_info_full
                    where dt = '$do_date') ovi
                       right join
                       (select appvideo.play_sec     play_duration,--播放时长
                               appvideo.position_sec watch_progress, --播放进度
                               appvideo.video_id,--视频id
                               common.uid            user_id,
                               ts
                        from ${APP}.ods_log_inc
                        where dt = '$do_date'
                          and appvideo.play_sec is not null
                          and err.error_code is null) oli
                   on ovi.id = oli.video_id
               )
         ) t1);
"
case $1 in
    "dwd_interaction_comment_add_inc" )
        hive -e "$dwd_interaction_comment_add_inc"
    ;;
    "dwd_interaction_review_add_inc" )
        hive -e "$dwd_interaction_review_add_inc"
    ;;
    "dwd_interaction_favor_add_inc" )
        hive -e "$dwd_interaction_favor_add_inc"
    ;;
    "dwd_exam_paper_exam_inc" )
        hive -e "$dwd_exam_paper_exam_inc"
    ;;
    "dwd_exam_question_exam_inc" )
        hive -e "$dwd_exam_question_exam_inc"
    ;;
    "dwd_trade_cart_add_inc" )
        hive -e "$dwd_trade_cart_add_inc"
    ;;   
    "dwd_trade_order_detail_inc" )
        hive -e "$dwd_trade_order_detail_inc"
    ;;  
    "dwd_trade_pay_detail_suc_inc" )
        hive -e "$dwd_trade_pay_detail_suc_inc"
    ;;
    "dwd_traffic_page_view_inc" )
        hive -e "$dwd_traffic_page_view_inc"
    ;;
    "dwd_play_user_chapter_process_inc" )
        hive -e "$dwd_play_user_chapter_process_inc"
    ;;   
    "dwd_play_video_play_inc" )
        hive -e "$dwd_play_video_play_inc"
    ;; 

    "dwd_user_login_inc" )
        hive -e "$dwd_user_login_inc"
    ;; 
    "dwd_user_register_inc" )
        hive -e "$dwd_user_register_inc"
    ;; 
     
    "all" )
        hive -e "$dwd_exam_question_exam_inc$dwd_interaction_comment_add_inc$dwd_interaction_review_add_inc$dwd_interaction_favor_add_inc$dwd_exam_paper_exam_inc$dwd_trade_cart_add_inc$dwd_trade_order_detail_inc$dwd_trade_pay_detail_suc_inc$dwd_traffic_page_view_inc$dwd_user_login_inc$dwd_user_register_inc$dwd_play_video_play_inc$dwd_play_user_chapter_process_inc"
    ;;
esac

