#!/bin/bash
APP=edu

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

#交易域用户粒度订单历史至今汇总事实表
dws_trade_user_order_td="
insert overwrite table ${APP}.dws_trade_user_order_td partition(dt='$do_date')
select
    nvl(old.user_id,new.user_id),
    if(new.user_id is not null and old.user_id is null,'$do_date',old.order_date_first),
    if(new.user_id is not null,'$do_date',old.order_date_last),
    nvl(old.order_count_td,0)+nvl(new.order_count_1d,0),
    nvl(old.total_amount_td,0)+nvl(new.order_total_amount_1d,0)
from
    (
        select
            user_id,
            order_date_first,
            order_date_last,
            order_count_td,
            total_amount_td
        from ${APP}.dws_trade_user_order_td
        where dt=date_add('$do_date',-1)
    )old
        full outer join
    (
        select
            user_id,
            order_count_1d,
            order_total_amount_1d
        from ${APP}.dws_trade_user_order_1d
        where dt='$do_date'
    )new
    on old.user_id=new.user_id;
"
#用户域用户粒度登录历史至今汇总事实表
dws_user_user_login_td="
insert overwrite table ${APP}.dws_user_user_login_td partition(dt='$do_date')
select
    nvl(old.user_id,new.user_id),
    if(new.user_id is null,old.login_date_last,'$do_date'),
    nvl(old.login_count_td,0)+nvl(new.login_count_1d,0)
from
(
    select
        user_id,
        login_date_last,
        login_count_td
    from ${APP}.dws_user_user_login_td
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        user_id,
        count(*) login_count_1d
    from ${APP}.dwd_user_login_inc
    where dt='$do_date'
    group by user_id
)new
on old.user_id=new.user_id;
"
case $1 in
    "dws_trade_user_order_td" )
        hive -e "$dws_trade_user_order_td"
    ;;
    "dws_user_user_login_td" )
        hive -e "$dws_user_user_login_td"
    ;;
    "all" )
        hive -e "$dws_trade_user_order_td$dws_user_user_login_td"
    ;;
esac