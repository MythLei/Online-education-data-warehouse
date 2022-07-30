#!/bin/bash
APP=edu

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi
ads_traffic_stats_by_source="
insert overwrite table ${APP}.ads_traffic_stats_by_source
select *
from ${APP}.ads_traffic_stats_by_source
union
select '$do_date'                                                          dt,
       recent_days,
       source_type                                                         source,
       cast(count(distinct (mid_id)) as bigint)                            uv_count,
       cast(avg(during_time_1d) / 1000 as bigint)                          avg_duration_sec,
       cast(avg(page_count_1d) as bigint)                                  avg_page_count,
       cast(count(*) as bigint)                                            sv_count,
       cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as decimal(16, 2)) bounce_rate
from ${APP}.dws_traffic_session_page_view_1d lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt >= date_add('$do_date', -recent_days + 1)
group by recent_days, source_type;
"
#路径分析
ads_page_path="
insert overwrite table ${APP}.ads_page_path
select *
from ${APP}.ads_page_path
union
select '$do_date' dt,
       source,
       nvl(target, 'null'),
       count(*)   path_count
from (select concat('step-', rn, ':', page_id)          source,
             concat('step-', rn + 1, ':', next_page_id) target
      from (select page_id,
                   lead(page_id, 1, null) over (partition by session_id order by view_time) next_page_id,
                   row_number() over (partition by session_id order by view_time)           rn
            from ${APP}.dwd_traffic_page_view_inc
            where dt = '$do_date') t1) t2
group by source, target;
"
#各来源下单统计
ads_order_stats_by_source="
insert overwrite table ${APP}.ads_order_stats_by_source
select *
from ${APP}.ads_order_stats_by_source
union
select '$do_date'  dt,
       recent_days,
       source_type source,
       total_amount
from ${APP}.dwd_trade_pay_detail_suc_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt >= date_add('$do_date', -recent_days + 1)
group by recent_days, source_type;
"
#用户变动统计
ads_user_change="
insert overwrite table ${APP}.ads_user_change
select *
from ${APP}.ads_user_change
union
select churn.dt,
       user_churn_count,
       user_back_count
from (select 'do_date' dt,
             count(*)  user_churn_count
      from ${APP}.dws_user_user_login_td
      where dt = 'do_date'
        and login_date_last = date_add('do_date', -7)) churn
         join
     (select 'do_date' dt,
             count(*)  user_back_count
      from (select user_id,
                   login_date_last
            from ${APP}.dws_user_user_login_td
            where dt = 'do_date') t1
               join
           (select user_id,
                   login_date_last login_date_previous
            from ${APP}.dws_user_user_login_td
            where dt = date_add('do_date', -1)) t2
           on t1.user_id = t2.user_id
      where datediff(login_date_last, login_date_previous) >= 8) back
     on churn.dt = back.dt;
"
#用户留存率
ads_user_retention="
insert overwrite table ${APP}.ads_user_retention
select * from ${APP}.ads_user_retention
union
select
'$do_date' dt,
login_date_first create_date,
datediff('$do_date',login_date_first) retention_day,
sum(if(login_date_last='$do_date',1,0)) retention_count,
count(*) new_user_count,
cast(sum(if(login_date_last='$do_date',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from
(
select
user_id,
date_id login_date_first
from ${APP}.dwd_user_register_inc
where dt>=date_add('$do_date',-7)
and dt<'$do_date'
)t1
join
(
select
user_id,
login_date_last
from ${APP}.dws_user_user_login_td
where dt='$do_date'
)t2
on t1.user_id=t2.user_id
group by login_date_first;
"
#用户新增活跃统计
ads_user_stats="
insert overwrite table ${APP}.ads_user_retention
select *
from ${APP}.ads_user_retention
union
select '$do_date'                                                                           dt,
       login_date_first                                                                     create_date,
       datediff('$do_date', login_date_first)                                               retention_day,
       sum(if(login_date_last = '$do_date', 1, 0))                                          retention_count,
       count(*)                                                                             new_user_count,
       cast(sum(if(login_date_last = '$do_date', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (select user_id,
             date_id login_date_first
      from ${APP}.dwd_user_register_inc
      where dt >= date_add('$do_date', -7)
        and dt < '$do_date') t1
         join
     (select user_id,
             login_date_last
      from ${APP}.dws_user_user_login_td
      where dt = '$do_date') t2
     on t1.user_id = t2.user_id
group by login_date_first;
"
#用户行为漏斗分析
ads_user_action="
insert overwrite table ${APP}.ads_user_action
select *
from ${APP}.ads_user_action
union
select '$do_date' dt,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from (select 1                                      recent_days,
             sum(if(page_id = 'home', 1, 0))        home_count,
             sum(if(page_id = 'good_detail', 1, 0)) good_detail_count
      from ${APP}.dws_traffic_page_visitor_page_view_1d
      where dt = '$do_date'
        and page_id in ('home', 'good_detail')) page
         join
     (select 1        recent_days,
             count(*) cart_count
      from ${APP}.dws_trade_user_cart_add_1d
      where dt = '$do_date') cart
     on page.recent_days = cart.recent_days
         join
     (select 1        recent_days,
             count(*) order_count
      from ${APP}.dws_trade_user_order_1d
      where dt = '$do_date') ord
     on page.recent_days = ord.recent_days
         join
     (select 1        recent_days,
             count(*) payment_count
      from ${APP}.dws_trade_user_payment_1d
      where dt = '$do_date') pay
     on page.recent_days = pay.recent_days;
	 "
#新增交易用户统计
ads_new_order_user_stats="
insert overwrite table ${APP}.ads_new_order_user_stats
select *
from ${APP}.ads_new_order_user_stats
union
select '$do_date'                                                              dt,
       recent_days,
       sum(if(order_date_first >= date_add('$do_date', -recent_days + 1), 1, 0)) new_order_user_count,
       count(distinct(user_id))                                                       new_pay_user_count
from ${APP}.dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '$do_date'
group by recent_days;
"
#各年龄段下单用户数
ads_agegroup_order_user_count="
insert overwrite table ${APP}.ads_agegroup_order_user_count
select *
from ${APP}.ads_agegroup_order_user_count
union
select '$do_date'            dt,
       recent_days,
       age_group,
       sum(order_user_count) user_count
from (
         select '$do_date' dt,
                t2.recent_days,
                t2.user_id,
                order_user_count,
                age_group
         from (select 1                       recent_days,
                      user_id,
                      count(distinct user_id) order_user_count
               from ${APP}.dws_trade_user_order_1d
               where dt = '$do_date'
               group by user_id
               union all
               select recent_days, user_id, count(distinct (if(order_count_nd > 0, user_id, null))) order_user_count
               from (select recent_days,
                            user_id,
                            case recent_days
                                when 7 then order_count_7d
                                when 30 then order_count_30d
                                end order_count_nd
                     from ${APP}.dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                     where dt = '$do_date') t1
               group by user_id, recent_days) t2
                  inner join
              (select recent_days,
                      user_id,
                      age_group,
                      count(distinct user_id)
               from (
                        select id      user_id,
                               recent_days,
                               case
                                   when (year(current_date()) - year(birthday)) > 50 then '50岁以上'
                                   when (year(current_date()) - year(birthday)) > 40 then '40岁-50岁'
                                   when (year(current_date()) - year(birthday)) > 30 then '31岁-40岁'
                                   when (year(current_date()) - year(birthday)) > 20 then '21岁-30岁'
                                   when (year(current_date()) - year(birthday)) > 0 then '20岁以下'
                                   end age_group
                        from ${APP}.dim_user_zip
                                 lateral view explode(array(1, 7, 30)) tmp as recent_days
                        where dt = '$do_date'
                    ) t3
               group by recent_days, age_group, user_id) t4
              on t4.user_id = t2.user_id
     ) tt
where dt = '$do_date'
group by recent_days, age_group, dt;"
#各分类课程交易统计
ads_order_stats_by_cate="
insert overwrite table ${APP}.ads_order_stats_by_cate
select dt
     , recent_days
     , category_id
     , category_name
     , order_count
     , order_user_count
     , order_amount
from (select dt,
             category_id,
             category_name,
             sum(course_id)             order_count,
             sum(order_user_num_1d)     order_user_count,
             sum(order_total_amount_1d) order_amount
      from (select course_id,
                   order_count_1d,
                   order_user_num_1d,
                   order_total_amount_1d,
                   dt
            from ${APP}.dws_trade_user_course_order_1d) a1
               left join (select id, --课程id
                                 category_id,
                                 category_name
                          from ${APP}.dim_course_info_full 

      ) b1 on a1.course_id = b1.id
      group by category_id, category_name, course_id, dt) a2 lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '$do_date'
  and dt >= date_sub('$do_date', recent_days - 1);
"
#各学科课程交易统计
ads_order_stats_by_sub="
insert overwrite table ${APP}.ads_order_stats_by_sub
select dt
     , recent_days
     , subject_id
     , subject_name
     , order_count
     , order_user_count
     , order_amount
from (select dt,
             subject_id, --学科id
             subject_name,
             sum(course_id)             order_count,
             sum(order_user_num_1d)     order_user_count,
             sum(order_total_amount_1d) order_amount
      from (select course_id, --课程id
                   order_count_1d,
                   order_user_num_1d,
                   order_total_amount_1d,
                   dt
            from ${APP}.dws_trade_user_course_order_1d) a1
               left join (select subject_id, --学科id
                                 subject_name,
                                 kc.id       --课程id
                          from (select id,          --课程id
                                       subject_id,  --学科id
                                       category_id, --分类id
                                       category_name
                                from ${APP}.dim_course_info_full --课程维度表
                               ) kc
                                   left join (select id,         --科目id
                                                     subject_name,
                                                     category_id --分类id
                                              from ${APP}.ods_base_subject_info_full) km
                                             on kc.category_id = km.category_id) b1 on a1.course_id = b1.id
      group by subject_id, subject_name, course_id, dt) a2 lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '$do_date'
  and dt >= date_sub('$do_date', recent_days - 1);
"
#各课程交易统计
ads_order_stats_by_course="
insert overwrite table ${APP}.ads_order_stats_by_course
select dt
     , recent_days
     , course_id
     , course_name
     , order_count
     , order_user_count
     , order_amount
from (select dt,
             course_id,
             course_name,
             sum(course_id)             order_count,
             sum(order_user_num_1d)     order_user_count,
             sum(order_total_amount_1d) order_amount
      from ${APP}.dws_trade_user_course_order_1d
      group by course_id, course_name, dt) a2 lateral view explode(array(1, 7, 30)) tmp as recent_days

where dt = '$do_date'
  and dt >= date_sub('$do_date', recent_days - 1);
"
#各课程评价统计
#各分类课程试听留存统计
#各学科试听留存统计
#各课程试听留存统计


#交易综合统计
ads_order_stats="
insert overwrite table ${APP}.ads_order_stats
select dt,
       recent_days,
       count(order_id)    order_count,
       count(user_id)     order_user_count,
       sum(origin_amount) order_total_amount
from ${APP}.dwd_trade_order_detail_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
group by dt, order_id,recent_days
having dt = '$do_date'
   and dt >= date_sub('$do_date', recent_days - 1);
"
#各省份交易统计
ads_order_by_province="
insert overwrite table ${APP}.ads_order_by_province
select dt
     , recent_days
     , province_id
     , province_name
     , area_code
     , iso_code
     , iso_code_3166_2
     , order_count
     , order_user_count
     , order_total_amount
from (select dt,
             a2.province_id,
             name       province_name,
             area_code,
             iso_code,
             iso_3166_2 iso_code_3166_2,
             order_count,
             order_user_count,
             order_total_amount
      from (select a1.user_id,
                   order_count,
                   order_total_amount,
                   order_user_count,
                   province_id --省份id
            from (select *
                  from (select user_id,
                               order_count_1d        order_count,
                               order_total_amount_1d order_total_amount
                        from ${APP}.dws_trade_user_order_1d
                        where dt = '$do_date') aa
                           join
                       (select count(user_id) order_user_count
                        from ${APP}.dws_trade_user_order_1d
                        where dt = '$do_date') bb on 1 = 1) a1
                     left join (select data.user_id,
                                       data.province_id
                                from ${APP}.ods_order_info_inc
                                where dt = '$do_date') b1 on a1.user_id = b1.user_id) a2
               left join (select id
                               , name
                               , region_id
                               , area_code
                               , iso_code
                               , iso_3166_2
                               , dt
                          from ${APP}.ods_base_province_full
                          where dt = '$do_date') b2
                         on a2.province_id = b2.id) a3 lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '$do_date'
  and dt >= date_sub('$do_date', recent_days - 1);
"
#各试卷相关指标统计
ads_test_paper_statistics="
insert overwrite table ${APP}.ads_test_paper_statistics
select '$do_date'                             dt,
       recent_days,
       test_paper_id,
       test_paper_title,
       cast(succ_avg_score as decimal(16, 2)) succ_avg_score,
       cast(succ_avg_time as decimal(16, 2))  succ_avg_time,
       succ_user_count
from (select 1                         recent_days,
             paper_id                  test_paper_id,
             paper_title               test_paper_title,
             avg(score_1d)             succ_avg_score,
             avg(duration_sec_1d)      succ_avg_time,
             count(distinct (user_id)) succ_user_count
      from ${APP}.dws_exam_user_paper_exam_1d
      where dt = '$do_date'
      group by paper_id, paper_title
      union all
      select recent_days,
             paper_id                                             test_paper_id,
             paper_title                                          test_paper_title,
             avg(paper_score)                                     succ_avg_score,
             avg(paper_duration)                                  succ_avg_time,
             count(distinct (if(paper_score > 0, user_id, null))) succ_user_count
      from (select recent_days,
                   user_id,
                   paper_id,
                   paper_title,
                   case recent_days
                       when 7 then score_7d
                       when 30 then score_30d
                       end paper_score,
                   case recent_days
                       when 7 then duration_sec_7d
                       when 30 then duration_sec_30d
                       end paper_duration
            from ${APP}.dws_exam_user_paper_exam_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '$do_date') t1
      group by recent_days, paper_id, paper_title) t2;
"
#各课程考试相关指标统计
ads_course_test_statistics="
insert overwrite table ${APP}.ads_course_test_statistics
select '$do_date'                             dt,
       recent_days,
       course_id,
       course_name,
       cast(succ_avg_score as decimal(16, 2)) succ_avg_score,
       cast(succ_avg_time as decimal(16, 2))  succ_avg_time,
       succ_user_count
from (select 1                         recent_days,
             course_id,
             course_name,
             avg(score_1d)             succ_avg_score,
             avg(duration_sec_1d)      succ_avg_time,
             count(distinct (user_id)) succ_user_count
      from ${APP}.dws_exam_user_paper_exam_1d
      where dt = '$do_date'
      group by course_id, course_name
      union all
      select recent_days,
             course_id,
             course_name,
             avg(paper_score)                                     succ_avg_score,
             avg(paper_duration)                                  succ_avg_time,
             count(distinct (if(paper_score > 0, user_id, null))) succ_user_count
      from (select recent_days,
                   user_id,
                   course_id,
                   course_name,
                   case recent_days
                       when 7 then score_7d
                       when 30 then score_30d
                       end paper_score,
                   case recent_days
                       when 7 then duration_sec_7d
                       when 30 then duration_sec_30d
                       end paper_duration
            from ${APP}.dws_exam_user_paper_exam_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '$do_date') t1
      group by recent_days, course_id, course_name) t2;
"
#各试卷分数分布统计
ads_paper_garde_section_statistics="
insert overwrite table ${APP}.ads_paper_garde_section_statistics
select dt,recent_days,
       paper_id,
       paper_title,
       garde_section,
       count(distinct (user_id)) user_count
from (select 1       recent_days,
             paper_id,
             paper_title,
             user_id,
			 dt,
             case
                 when score_1d < 60 then '0-60'
                 when score_1d >= 60 and score_1d < 70 then '60-70'
                 when score_1d >= 70 and score_1d < 80 then '70-80'
                 when score_1d >= 80 and score_1d < 90 then '80-90'
                 when score_1d >= 90 then '90-100'
                 end garde_section
      from ${APP}.dws_exam_user_paper_exam_1d
      where dt = '$do_date'
      union all
      select 7       recent_days,
             paper_id,
             paper_title,
             user_id,
			 dt,
             case
                 when score_7d < 60 then '0-60'
                 when score_7d >= 60 and score_7d < 70 then '60-70'
                 when score_7d >= 70 and score_7d < 80 then '70-80'
                 when score_7d >= 80 and score_7d < 90 then '80-90'
                 when score_7d >= 90 then '90-100'
                 end garde_section
      from ${APP}.dws_exam_user_paper_exam_nd
      where dt = '$do_date'
      union all
      select 30      recent_days,
             paper_id,
             paper_title,
             user_id,
			 dt,
             case
                 when score_30d < 60 then '0-60'
                 when score_30d >= 60 and score_30d < 70 then '60-70'
                 when score_30d >= 70 and score_30d < 80 then '70-80'
                 when score_30d >= 80 and score_30d < 90 then '80-90'
                 when score_30d >= 90 then '90-100'
                 end garde_section
      from ${APP}.dws_exam_user_paper_exam_nd
      where dt = '$do_date') garde_sec
group by recent_days, paper_id, paper_title, garde_section,dt;
"
#各题目正确率统计
ads_topic_accuracy_statistics="
insert overwrite table ${APP}.ads_topic_accuracy_statistics
select d1.dt,d1.recent_days,
       d1.question_id,
       e1.question_txt,
       cast(succ_count / all_count as decimal(16, 2)) succ_rate
from (select a1.recent_days recent_days,
             a1.question_id question_id,dt,
             succ_count,
             all_count
      from (select recent_days,
                   question_id,dt,
                   is_correct,
                   if(is_correct = '1', sum(if(date_id >= date_add('$do_date', -recent_days + 1), 1, 0)), 0) succ_count
            from ${APP}.dwd_exam_question_exam_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
            where dt = '$do_date'
              and is_correct = '1'
            group by recent_days, question_id,dt, is_correct) a1
               join
           (select recent_days,
                   question_id,dt,
                   sum(if(date_id >= date_add('$do_date', -recent_days + 1), 1, 0)) all_count
            from ${APP}.dwd_exam_question_exam_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
            where dt = '$do_date'
            group by recent_days, dt,question_id) c1
           on a1.question_id = c1.question_id
      group by a1.recent_days, a1.question_id, succ_count,a1.dt, all_count) d1
         left join
     (select question_id,dt,
             question_txt
      from ${APP}.dim_question_full
      where dt = '$do_date'
      group by question_id, dt,question_txt) e1
     on d1.question_id = e1.question_id;
"
#播放主题指标
ads_chapter_video_play_statistics="
insert overwrite table ${APP}.ads_chapter_video_play_statistics
select dt,
       recent_day,
       course_id,
       course_name,
       video_play_times,
       avg_watch_time,
       watch_count
from (select 1                                                     recent_day,
             course_id                                             course_id,
             course_name,
             sum(view_video_count_1d)                              video_play_times,
             sum(during_video_time_1d) / count(distinct (user_id)) avg_watch_time,
             count(distinct (user_id))                             watch_count,
             dt
      from ${APP}.dws_user_video_play_1d
      where dt = '$do_date'
      group by course_id, course_name,dt
      union
      select recent_days,
             course_id,
             course_name,
             sum(video_play_times),
             sum(avg_watch_time) / count(distinct (if(watch_count > 0, user_id, 0))),
             count(distinct (if(watch_count > 0, user_id, 0))),
             dt
      from (select recent_days,
                   user_id,
                   course_id,
                   course_name,
                   dt,
                   case recent_days
                       when 7 then view_video_count_7d
                       when 30 then view_video_count_30d
                       end video_play_times,
                   case recent_days
                       when 7 then during_video_time_7d
                       when 30 then during_video_time_30d
                       end avg_watch_time,
                   case recent_days
                       when 7 then view_video_progress_7d
                       when 30 then view_video_progress_30d
                       end watch_count
            from ${APP}.dws_user_video_play_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '$do_date') b1
      group by recent_days, course_id, course_name,dt) a1;
"
#各课程视频播放情况统计
ads_course_video_play_statistics="
insert overwrite table ${APP}.ads_course_video_play_statistics
select dt,recent_day,
       course_id,
       course_name,
       video_play_times,
       avg_watch_time,
       watch_count
from (select 1                                                     recent_day,
             course_id                                             course_id,dt,
             course_name,
             sum(view_video_count_1d)                              video_play_times,
             sum(during_video_time_1d) / count(distinct (user_id)) avg_watch_time,
             count(distinct (user_id))                             watch_count
      from ${APP}.dws_user_video_play_1d
      where dt = '$do_date'
      group by course_id, dt,course_name
      union
      select recent_days,
             course_id,
             course_name,dt,
             sum(video_play_times),
             sum(avg_watch_time) / count(distinct (if(watch_count > 0, user_id, 0))),
             count(distinct (if(watch_count > 0, user_id, 0)))
      from (select recent_days,
                   user_id,
                   course_id,
                   course_name,dt,
                   case recent_days
                       when 7 then view_video_count_7d
                       when 30 then view_video_count_30d
                       end video_play_times,
                   case recent_days
                       when 7 then during_video_time_7d
                       when 30 then during_video_time_30d
                       end avg_watch_time,
                   case recent_days
                       when 7 then view_video_progress_7d
                       when 30 then view_video_progress_30d
                       end watch_count
            from ${APP}.dws_user_video_play_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '$do_date') b1
      group by recent_days, course_id,dt, course_name) a1;
"
#各课程完课人数统计
ads_course_over_user_statistics="
insert overwrite table ${APP}.ads_course_over_user_statistics
select '$do_date' dt,
       1          recent_days,
       course_id,
       course_name,
       sum(if(flag_course = 0, 1, 0))
from (select course_id,
             course_name,
             user_id,
             sum(flag_video) over (partition by course_id,user_id) flag_course
      from (select user_id,
                   course_id,
                   course_name,
                   if(during_video_time_1d > 900 * 0.9 and view_video_progress_1d > 900 * 0.9, 0, 1) flag_video
            from ${APP}.dws_user_video_play_1d
            where dt = '$do_date') t1) t2
group by course_id,
         course_name
union all
select '$do_date' dt,
       recent_days,
       course_id,
       course_name,
       sum(if(flag_course = 0, 1, 0))
from (select recent_days,
             course_id,
             course_name,
             sum(flag_video) over (partition by course_id,user_id) flag_course
      from (select recent_days,
                   course_id,
                   course_name,
                   user_id,
                   if(play_sum > 900 * 0.9 and pos_max > 900 * 0.9, 0, 1) flag_video
            from (select recent_days,
                         course_id,
                         course_name,
                         user_id,
                         case recent_days
                             when 7 then during_video_time_7d
                             when 30 then during_video_time_30d
                             end play_sum,
                         case recent_days
                             when 7 then view_video_progress_7d
                             when 30 then view_video_progress_30d
                             end pos_max
                  from ${APP}.dws_user_video_play_nd
                           lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '$do_date') t3) t4) t5
group by recent_days,
         course_id,
         course_name;
"
#完课综合指标
ads_all_course_over_user_statistics="
insert overwrite table ${APP}.ads_all_course_over_user_statistics
select '$do_date' dt,
       1          recent_days,
       count(*),
       count(distinct user_id)
from (select course_id,
             course_name,
             user_id,
             sum(flag_video) over (partition by course_id,user_id) flag_course
      from (select user_id,
                   course_id,
                   course_name,
                   if(during_video_time_1d > 900 * 0.9 and view_video_progress_1d > 900 * 0.9, 0, 1) flag_video
            from ${APP}.dws_user_video_play_1d
            where dt = '$do_date') t1) t2
where flag_course = 0
union all
select '$do_date' dt,
       recent_days,
       count(*),
       count(distinct user_id)
from (select recent_days,
             course_id,
             course_name,
             user_id,
             sum(flag_video) over (partition by course_id,user_id) flag_course
      from (select recent_days,
                   course_id,
                   course_name,
                   user_id,
                   if(play_sum > 900 * 0.9 and pos_max > 900 * 0.9, 0, 1) flag_video
            from (select recent_days,
                         course_id,
                         course_name,
                         user_id,
                         case recent_days
                             when 7 then during_video_time_7d
                             when 30 then during_video_time_30d
                             end play_sum,
                         case recent_days
                             when 7 then during_video_time_7d
                             when 30 then during_video_time_30d
                             end pos_max
                  from ${APP}.dws_user_video_play_nd
                           lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '$do_date') t3) t4) t5
where flag_course = 0
group by recent_days;
"
#各课程人均完成章节视频数统计
ads_course_avg_user_chapter_finished_statistics="
insert overwrite table ${APP}.ads_course_avg_user_chapter_finished_statistics
select '$do_date' dt,
       1          recent_days,
       course_id,
       course_name,
       cast(sum(flag_video) / count(distinct user_id) as decimal(16, 2))
from (select user_id,
             chapter_id,
             course_id,
             course_name,
             if(during_video_time_1d > 900 * 0.9 and view_video_progress_1d > 900 * 0.9, 1, 0) flag_video
      from ${APP}.dws_user_video_play_1d
      where dt = '$do_date') t1
group by course_id,
         course_name
union all
select '$do_date' dt,
       recent_days,
       course_id,
       course_name,
       cast(sum(flag_video) / count(distinct user_id) as decimal(16, 2))
from (select recent_days,
             course_id,
             course_name,
             user_id,
             if(play_sum > 900 * 0.9 and pos_max > 900 * 0.9, 1, 0) flag_video
      from (select recent_days,
                   course_id,
                   course_name,
                   user_id,
                   case recent_days
                       when 7 then during_video_time_7d
                       when 30 then during_video_time_30d
                       end play_sum,
                   case recent_days
                       when 7 then view_video_progress_7d
                       when 30 then view_video_progress_30d
                       end pos_max
            from ${APP}.dws_user_video_play_nd
                     lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '$do_date') t3) t4
group by recent_days,
         course_id,
         course_name;
"
case $1 in
    "ads_traffic_stats_by_source" )
        hive -e "$ads_traffic_stats_by_source"
    ;;
    "ads_page_path" )
        hive -e "$ads_page_path"
    ;;
    "ads_order_stats_by_source" )
        hive -e "$ads_order_stats_by_source"
    ;;
    "ads_user_change" )
        hive -e "$ads_user_change"
    ;;
    "ads_user_retention" )
        hive -e "$ads_user_retention"
    ;;
    "ads_user_stats" )
        hive -e "$ads_user_stats"
    ;;
    "ads_user_action" )
        hive -e "$ads_user_action"
    ;;
    "ads_new_order_user_stats" )
        hive -e "$ads_new_order_user_stats"
    ;;
    "ads_agegroup_order_user_count" )
        hive -e "$ads_agegroup_order_user_count"
    ;;
    "ads_order_stats_by_cate" )
        hive -e "$ads_order_stats_by_cate"
    ;;
    "ads_order_stats_by_sub" )
        hive -e "$ads_order_stats_by_sub"
    ;;
    "ads_order_stats_by_course" )
        hive -e "$ads_order_stats_by_course"
    ;;
    "ads_order_stats" )
        hive -e "$ads_order_stats"
    ;;
    "ads_order_by_province" )
        hive -e "$ads_order_by_province"
    ;;
    "ads_test_paper_statistics" )
        hive -e "$ads_test_paper_statistics"
    ;;
	"ads_course_test_statistics" )
        hive -e "$ads_course_test_statistics"
    ;;
    "ads_paper_garde_section_statistics" )
        hive -e "$ads_paper_garde_section_statistics"
    ;;
	"ads_topic_accuracy_statistics" )
        hive -e "$ads_topic_accuracy_statistics"
    ;;
	"ads_chapter_video_play_statistics" )
        hive -e "$ads_chapter_video_play_statistics"
    ;;
	"ads_course_video_play_statistics" )
        hive -e "$ads_course_video_play_statistics"
    ;;
	"ads_course_over_user_statistics" )
        hive -e "$ads_course_over_user_statistics"
    ;;
	"ads_all_course_over_user_statistics" )
        hive -e "$ads_all_course_over_user_statistics"
    ;;
	"ads_course_avg_user_chapter_finished_statistics" )
        hive -e "$ads_course_avg_user_chapter_finished_statistics"
    ;;
    "all" )
        hive -e "$ads_traffic_stats_by_source$ads_page_path$ads_user_change$ads_user_retention$ads_user_stats$ads_user_action$ads_new_order_user_stats$ads_agegroup_order_user_count$ads_order_stats_by_cate$ads_order_stats_by_sub$ads_order_stats_by_course$ads_order_stats$ads_order_by_province$ads_course_test_statistics$ads_paper_garde_section_statistics$ads_topic_accuracy_statistics$ads_chapter_video_play_statistics$ads_course_video_play_statistics$ads_course_over_user_statistics$ads_all_course_over_user_statistics$ads_course_avg_user_chapter_finished_statistics"
    ;;
esac

