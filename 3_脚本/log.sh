#!/bin/bash
for i in hadoop101 hadoop102; do
    echo "========== $i =========="
    ssh $i "nohup /opt/module/data_mocker/; java -jar edu2021-mock-2022-06-18.jar >/dev/null 2>&1 &"
done 
