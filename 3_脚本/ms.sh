#!/bin/bash
echo "====================> hadoop102启动 metastore <===================="
ssh hadoop102 'nohup hive --service metastore > /opt/module/hive/logs/ms.log 2>&1 &'
