#! /bin/bash

DATAX_HOME=/opt/module/datax

#DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_export_path(){
  for i in `hadoop fs -ls -R $1 | awk '{print $8}'`; do
    hadoop fs -test -z $i
    if [[ $? -eq 0 ]]; then
      echo "$i文件大小为0，正在删除"
      hadoop fs -rm -r -f $i
    fi
  done
}

#数据导出
export_data() {
  datax_config=$1
  export_dir=$2
  handle_export_path $export_dir
  $DATAX_HOME/bin/datax.py -p"-Dexportdir=$export_dir" $datax_config
}

case $1 in
  "ads_traffic_stats_by_source")
    export_data /opt/module/datax/job/export/edu_report.ads_ads_traffic_stats_by_source /warehouse/edu/ads/ads_traffic_stats_by_source
  ;;
  "ads_page_path")
    export_data /opt/module/datax/job/export/edu_report.ads_page_path.json /warehouse/edu/ads/ads_page_path
  ;;  
  "ads_order_stats_by_source")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_source.json /warehouse/edu/ads/ads_order_stats_by_source
  ;;
  "ads_user_change")
    export_data /opt/module/datax/job/export/edu_report.ads_user_change.json /warehouse/edu/ads/ads_user_change
  ;;
  "ads_user_retention")
    export_data /opt/module/datax/job/export/edu_report.ads_user_retention.json /warehouse/edu/ads/ads_user_retention
  ;;
  "ads_user_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_user_stats.json /warehouse/edu/ads/ads_user_stats
  ;;  
  "ads_user_action")
    export_data /opt/module/datax/job/export/edu_report.ads_user_action.json /warehouse/edu/ads/ads_user_action
  ;;
  "ads_new_order_user_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_new_order_user_stats.json /warehouse/edu/ads/ads_new_order_user_stats
  ;;
  "ads_agegroup_order_user_count")
    export_data /opt/module/datax/job/export/edu_report.ads_agegroup_order_user_count.json /warehouse/edu/ads/ads_agegroup_order_user_count
  ;;
  "ads_order_stats_by_cate")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_cate.json /warehouse/edu/ads/ads_order_stats_by_cate
  ;;  
  "ads_order_stats_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_course.json /warehouse/edu/ads/ads_order_stats_by_course
  ;;
  "ads_order_stats_by_sub")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_sub.json /warehouse/edu/ads/ads_order_stats_by_sub
  ;;
  "ads_review_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_review_stats.json /warehouse/edu/ads/ads_review_stats
  ;;
  "ads_audition_stats_by_cate")
    export_data /opt/module/datax/job/export/edu_report.ads_audition_stats_by_cate.json /warehouse/edu/ads/ads_audition_stats_by_cate
  ;;  
  "ads_audition_stats_by_subject")
    export_data /opt/module/datax/job/export/edu_report.ads_audition_stats_by_subject.json /warehouse/edu/ads/ads_audition_stats_by_subject
  ;;
  "ads_audition_stats_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_audition_stats_by_course.json /warehouse/edu/ads/ads_audition_stats_by_course
  ;;
    "ads_order_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats.json /warehouse/edu/ads/ads_order_stats
  ;;
    "ads_order_by_province")
    export_data /opt/module/datax/job/export/edu_report.ads_order_by_province.json /warehouse/edu/ads/ads_order_by_province
  ;;
    "ads_test_paper_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_test_paper_statistics.json /warehouse/edu/ads/ads_test_paper_statistics
  ;;
    "ads_course_test_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_course_test_statistics.json /warehouse/edu/ads/ads_course_test_statistics
  ;;
    "ads_paper_garde_section_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_paper_garde_section_statistics.json /warehouse/edu/ads/ads_paper_garde_section_statistics
  ;;
    "ads_topic_accuracy_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_topic_accuracy_statistics.json /warehouse/edu/ads/ads_topic_accuracy_statistics
  ;;
    "ads_chapter_video_play_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_chapter_video_play_statistics.json /warehouse/edu/ads/ads_chapter_video_play_statistics
  ;;
    "ads_course_video_play_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_course_video_play_statistics.json /warehouse/edu/ads/ads_course_video_play_statistics
  ;;
    "ads_all_course_over_user_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_all_course_over_user_statistics.json /warehouse/edu/ads/ads_all_course_over_user_statistics
  ;;
   "ads_course_avg_user_chapter_finished_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_course_avg_user_chapter_finished_statistics.json /warehouse/edu/ads/ads_course_avg_user_chapter_finished_statistics
  ;;
   "ads_course_over_user_statistics")
    export_data /opt/module/datax/job/export/edu_report.ads_course_over_user_statistics.json /warehouse/edu/ads/ads_course_over_user_statistics
  ;;
  "all")
    export_data /opt/module/datax/job/export/edu_report.ads_traffic_stats_by_source.json /warehouse/edu/ads/ads_traffic_stats_by_source
    export_data /opt/module/datax/job/export/edu_report.ads_page_path /warehouse/edu/ads/ads_page_path
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_source.json /warehouse/edu/ads/ads_order_stats_by_source
    export_data /opt/module/datax/job/export/edu_report.ads_user_change.json /warehouse/edu/ads/ads_user_change
    export_data /opt/module/datax/job/export/edu_report.ads_user_retention.json /warehouse/edu/ads/ads_user_retention
    export_data /opt/module/datax/job/export/edu_report.ads_user_stats.json /warehouse/edu/ads/ads_user_stats
    export_data /opt/module/datax/job/export/edu_report.ads_user_action.json /warehouse/edu/ads/ads_user_action
    export_data /opt/module/datax/job/export/edu_report.ads_new_order_user_stats.json /warehouse/edu/ads/ads_new_order_user_stats
    export_data /opt/module/datax/job/export/edu_report.ads_agegroup_order_user_count.json /warehouse/edu/ads/ads_agegroup_order_user_count
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_cate.json /warehouse/edu/ads/ads_order_stats_by_cate
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_course.json /warehouse/edu/ads/ads_order_stats_by_course
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_sub.json /warehouse/edu/ads/ads_order_stats_by_sub
    export_data /opt/module/datax/job/export/edu_report.ads_review_stats.json /warehouse/edu/ads/ads_review_stats
    export_data /opt/module/datax/job/export/edu_report.ads_audition_stats_by_cate.json /warehouse/edu/ads/ads_audition_stats_by_cate
    export_data /opt/module/datax/job/export/edu_report.ads_audition_stats_by_subject.json /warehouse/edu/ads/ads_audition_stats_by_subject
    export_data /opt/module/datax/job/export/edu_report.ads_audition_stats_by_course.json /warehouse/edu/ads/ads_audition_stats_by_course
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats.json /warehouse/edu/ads/ads_order_stats
    export_data /opt/module/datax/job/export/edu_report.ads_order_by_province.json /warehouse/edu/ads/ads_order_by_province
    export_data /opt/module/datax/job/export/edu_report.ads_test_paper_statistics.json /warehouse/edu/ads/ads_test_paper_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_course_test_statistics.json /warehouse/edu/ads/ads_course_test_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_paper_garde_section_statistics.json /warehouse/edu/ads/ads_paper_garde_section_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_topic_accuracy_statistics.json /warehouse/edu/ads/ads_topic_accuracy_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_chapter_video_play_statistics.json /warehouse/edu/ads/ads_chapter_video_play_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_course_video_play_statistics.json /warehouse/edu/ads/ads_course_video_play_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_all_course_over_user_statistics.json /warehouse/edu/ads/ads_all_course_over_user_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_course_avg_user_chapter_finished_statistics.json /warehouse/edu/ads/ads_course_avg_user_chapter_finished_statistics
    export_data /opt/module/datax/job/export/edu_report.ads_course_over_user_statistics.json /warehouse/edu/ads/ads_course_over_user_statistics
  ;;
esac
