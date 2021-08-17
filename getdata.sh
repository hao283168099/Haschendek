#! /bin/bash
#按库来区分不同的list文件
#list文件内部的目录的格式，是hdfs内部路径，即：应去掉hdfs://namenodeIP，以/开头，结尾不能有/
#例如/user/hive/warehouse/adtec.db(库级get，包含该库目录及所有表)或者/user/hive/warehouse/adtec.db/a01（表级get，包含该表目录及所有分区）
#或者/user/hive/warehouse/adtec.db/a01/dt=20210812（分区级get，包含指定分区目录及目录下所有文件）

printUsage()
{
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] """
    tmp="sh $0 -n FileName -i NameNodeIP -l ListPath -s StorePath -d DataBaseName [-t TableName] "
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""用法: $tmp"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""-n FileName: list文件名称.--> 必输"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""-i NameNodeIP: HDFS主namenode的IP地址.--> 必输"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""-l ListPath: list文件所在目录路径，路径以/开头，最后不能加/ 例如/tmp/listfile--> 必输"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""-s StorePath: 将要保存的data数据表文件所在目录路径，路径以/开头，最后不能加/ 例如/tmp/datagot--> 必输"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""-d DataBaseName: 将要保存的data数据表文件所属数据库名称 --> 必输"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""-t TableName: 将要保存的数据表分区的文件，所属表的名称 --> 非必输"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""请将list文件放至/tmp/list目录下"
	echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] """
    tmp='sh getdata.sh -n list_adtec -i 10.10.10.10 -l /tmp/listfile -s /tmp/datagot -d adtec'
	echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""举例: $tmp"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] """
}

fileName=""
nnip=""
listpath=""
storepath=""
databasename=""
tablename=""

while getopts ":n:i:l:s:d:t:" opt
do
    case $opt in
        n)
        fileName=$OPTARG
        ;;
        i)
        nnip=$OPTARG
        ;;
        l)
        listpath=$OPTARG
        ;;
        s)
        storepath=$OPTARG
        ;;
        d)
        databasename=$OPTARG
        ;;
        t)
        tablename=$OPTARG
        ;;
        ?)
        printUsage
        exit $SUCC
        ;;
    esac
done

if [ -z /tmp/list/$fileName ]; then
  printUsage
  exit $FAIL
fi

if [ -z $nnip ]; then
  printUsage
  exit $FAIL
fi

if [ -z $listpath ]; then
  printUsage
  exit $FAIL
fi

if [ -z $storepath ]; then
  printUsage
  exit $FAIL
fi

if [ -z $databasename ]; then
  printUsage
  exit $FAIL
fi

if [ ! -f $listpath/$fileName ]; then
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] ""File $listpath/$fileName not found!"
    echo "Info[$(date +%Y%m%d%H%M%S):$LINENO] """
  exit $FAIL
fi

if [ -z $tablename ]; then
    mkdir -p $storepath/hivedata/$databasename

    cat $listpath/$fileName |while read eachline
    do
    hadoop fs -get hdfs://$nnip:25000$eachline $storepath/hivedata/$databasename/
    done

else

    mkdir -p $storepath/hivedata/event_par/$databasename/$tablename

    cat $listpath/$fileName |while read eachline
    do
    hadoop fs -get hdfs://$nnip:25000$eachline $storepath/hivedata/event_par/$databasename/$tablename/
    done
fi
