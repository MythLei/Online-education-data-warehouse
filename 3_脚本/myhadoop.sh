#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input..."
    exit ;
fi
case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="

        echo " --------------- 启动 hdfs ---------------"
        echo "Starting namenodes on hadoop101"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start namenode"
        echo "Starting namenodes on hadoop102"
        ssh hadoop102 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start namenode"
        echo "Starting datanodes"
        ssh hadoop103 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start datanode"
        ssh hadoop104 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start datanode"
        ssh hadoop105 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start datanode"
        echo "Starting journalnode"
        ssh hadoop103 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start journalnode"
        ssh hadoop104 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start journalnode"
        ssh hadoop105 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start journalnode"
        echo " --------------- 启动 yarn ---------------"
        echo "Starting resourcemanagers on hadoop101"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon start resourcemanager"
        echo "Starting resourcemanagers on hadoop102"
        ssh hadoop102 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon start resourcemanager"
        echo "Starting nodemanager"
        ssh hadoop103 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon start nodemanager"
        ssh hadoop104 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon start nodemanager"
        ssh hadoop105 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon start nodemanager"
	echo " --------------- 启动 historyserver ---------------"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/mapred --daemon start historyserver"
        echo " --------------- 启动 zkfc ---------------"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon start zkfc"
	
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="


        echo " --------------- 关闭 yarn ---------------"
        echo "Stopping nodemanager"
        ssh hadoop103 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon stop nodemanager"
        ssh hadoop104 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon stop nodemanager"
        ssh hadoop105 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon stop nodemanager"
        echo "Stopping resourcemanagers on hadoop101"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon stop resourcemanager"
        echo "Stopping resourcemanagers on hadoop102"
        ssh hadoop102 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/yarn --daemon stop resourcemanager"
        echo " --------------- 关闭 hdfs ---------------"
        echo "Stopping journalnode"
        ssh hadoop103 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop journalnode"
        ssh hadoop104 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop journalnode"
        ssh hadoop105 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop journalnode"
        echo "Stopping namenodes on hadoop101"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop namenode"
        echo "Stopping namenodes on hadoop102"
        ssh hadoop102 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop namenode"
        echo "Stopping datanode"
        ssh hadoop103 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop datanode"
        ssh hadoop104 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop datanode"
        ssh hadoop105 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop datanode"
	echo " --------------- 关闭  historyserver ---------------"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/mapred --daemon stop historyserver"
        echo " --------------- 关闭 zkfc ---------------"
        ssh hadoop101 "source /etc/profile;/opt/module/hadoop-3.1.3/bin/hdfs --daemon stop zkfc"
;;
*)
    echo "Input Args Error..."
;;
esac

