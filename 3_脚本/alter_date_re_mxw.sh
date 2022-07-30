#!/bin/bash
#根据传递的日期参数修改配置文件的日期

MAXWELL_HOME=/opt/module/maxwell

if [ $# -ge 1 ]
then               
#c 替换  整行
	sed -i "/mock_date/c mock_date=$1" /opt/module/maxwell/config.properties
	sed -i "/mock.date/c mock.date: $1" /opt/module/data_mocker/application.yml
	
	result=`ps -ef | grep com.zendesk.maxwell.Maxwell | grep -v grep | wc -l`
    if [[ $result -gt 0 ]]; then
        echo "停止Maxwell"
        ps -ef | grep com.zendesk.maxwell.Maxwell | grep -v grep | awk '{print $2}' | xargs kill -9
		sleep 3
		echo "启动Maxwell"
        $MAXWELL_HOME/bin/maxwell --config $MAXWELL_HOME/config.properties --daemon
    else
        echo "Maxwell未在运行"
        $MAXWELL_HOME/bin/maxwell --config $MAXWELL_HOME/config.properties --daemon
		echo "启动Maxwell"
    fi
fi
echo "日期已修改,maxwell已重启"
