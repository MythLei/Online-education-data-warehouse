#!/bin/bash
#首日脚本
APP=edu

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
   echo "请传入日期参数"
   exit
fi

# 用户
dim_user_zip="
insert overwrite table ${APP}.dim_user_zip partition (dt='9999-12-31')
select
    data.id,
    data.login_name,
    data.nick_name,
    md5(data.real_name),
    md5(if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\\d{8}$',data.phone_num,null)),
    md5(if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\\.[a-zA-Z0-9_-]+)+$',data.email,null)),
    data.user_level,
    data.birthday,
    data.gender,
    data.create_time,
    data.operate_time,
    '2022-02-21' start_date,
    '9999-12-31' end_date
from ${APP}.ods_user_info_inc
where dt='$do_date'
and type='bootstrap-insert';
"
#地区维度表
dim_province_full="
insert overwrite table ${APP}.dim_province_full partition(dt='$do_date')
select
id,
name,
region_id,
area_code,
iso_code,
iso_3166_2
from ${APP}.ods_base_province_full
where dt='$do_date';
"

# 课程维度表
dim_course_info_full="
insert overwrite table ${APP}.dim_course_info_full partition (dt = '$do_date')
select course.id,
       course_name,
       subject_id,
       teacher,
       chapter_num,
       origin_price,
       reduce_amount,
       actual_price,
       course_introduce,
       create_time,
       update_time,
       subject_name,
       category_id,
       category_name
from (
         select id,
                course_name,
                subject_id,
                teacher,
                chapter_num,
                origin_price,
                reduce_amount,
                actual_price,
                course_introduce,
                create_time,
                update_time
         from ${APP}.ods_course_info_full
         where dt = '$do_date') course
         left join
     (
         select id,
                subject_name,
                category_id
         from ${APP}.ods_base_subject_info_full
         where dt = '$do_date'
     ) base
     on course.subject_id = base.id
         left join
     (
         select id,
                category_name
         from ${APP}.ods_base_category_info_full
         where dt = '$do_date'
     ) category
     on base.category_id = category.id
where base.id is not null;
" 
# 章节维度表
dim_chapter_full="
insert overwrite table ${APP}.dim_chapter_full partition (dt = '$do_date')
select chapter.id,
       chapter_name,
       video_id,
       is_free,
       video_name,
       create_time,
       update_time
from (
         select id,
                chapter_name,
                video_id,
                is_free,
                create_time,
                update_time
         from ${APP}.ods_chapter_info_full
         where dt = '$do_date'
     ) chapter
         full outer join
     (
         select id,
                video_name
         from ${APP}.ods_video_info_full
         where dt = '$do_date'
     ) video
     on chapter.id = video.id;
"
#问题信息维度表
dim_question_full="
insert overwrite table ${APP}.dim_question_full partition (dt = '$do_date')
select question.id,
       question_txt,
       chapter_id,
       course_id,
       question_type,
       create_time,
       update_time,
       option_txt,
       is_correct,
       question_id
from (
         select id,
                question_txt,
                chapter_id,
                course_id,
                question_type,
                create_time,
                update_time
         from ${APP}.ods_test_question_info_full
         where dt = '$do_date'
     ) question
         left join
     (
         select 
                option_txt,
                is_correct,
                question_id
         from ${APP}.ods_test_question_option_full
         where dt = '$do_date'
     ) option
     on question.id = option.question_id;
"

# 知识点维度表
dim_test_point_question_full="
insert overwrite table ${APP}.dim_test_point_question_full partition (dt = '$do_date')
select
       a1.id         
       ,point_id     
       ,question_id  
       ,publisher_id 
       ,create_time  
       ,update_time  
       ,point_txt    
       ,point_level  
       ,course_id    
       ,chapter_id   
from (select
         id,
         point_id,
         question_id
         from ${APP}.ods_test_point_question_full) a1
     right join
     (
     select
         id            
        ,publisher_id  
        ,create_time   
        ,update_time   
        ,point_txt     
        ,point_level   
        ,course_id     
        ,chapter_id    
    from ${APP}.ods_knowledge_point_full   
        ) a2
      on a1.point_id= a2.id; 
"

# 试卷维度表
dim_paper_full="
insert overwrite table ${APP}.dim_paper_full partition (dt = '$do_date')
select id,
       paper_title,
       course_id,
       create_time,
       update_time,
       publisher_id
from ${APP}.ods_test_paper_full
where dt = '$do_date';
"
# 时间维度表
dim_date="
insert overwrite table ${APP}.dim_date
select *
from ${APP}.tmp_dim_date_info;	 
"
case $1 in
"dim_user_zip")
    hive -e "$dim_user_zip"
;;
"dim_province_full")
    hive -e "$dim_province_full"
;;
"dim_course_info_full")
    hive -e "$dim_course_info_full"
;;
"dim_chapter_full")
    hive -e "$dim_chapter_full"
;;
"dim_question_full")
    hive -e "$dim_question_full"
;;
"dim_test_point_question_full")
    hive -e "$dim_test_point_question_full"
;;
"dim_paper_full")

    hive -e "$dim_paper_full"
;;
"dim_date")
    hive -e "$dim_date"
;;
"all")
    hive -e "$dim_province_full$dim_user_zip$dim_course_info_full$dim_chapter_full$dim_question_full$dim_test_point_question_full$dim_paper_full$dim_date"
;;
esac
