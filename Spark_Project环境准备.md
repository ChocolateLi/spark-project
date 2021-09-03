------



# Spark-Project环境准备

## 软件安装

Oracle_VM_VirtualBox的安装



## 环境安装和配置

### CentOS-7安装

1、使用课程提供的镜像：CentOS-7-x86_64-Minimal-2009.iso。

2、创建虚拟机：打开Virtual Box，点击“新建”按钮；输入虚拟机名称为sparkproject1，选择操作系统为Linux，选择版本为Red Hat；分配1024MB内存；后面所有选项全部用默认的设置；注意，在Virtual disk file location and size中，一定要自己选择一个目录来存放虚拟机文件；最后点击“create”按钮，开始创建虚拟机。

3、设置网卡（桥接网卡）：选择创建好的虚拟机，点击“设置”按钮，在网络一栏中，连接方式中，选择“Bridged Adapter”，即桥接网卡。

4、安装CentOS7操作系统：选择创建好的虚拟机，点击“开始”按钮；选择安装介质（即本地的CentOS 7镜像文件）；选择第一项开始安装-Skip-欢迎界面Next-选择默认语言-Baisc Storage Devices-Yes, discard any data-主机名:sparkproject1-选择时区-设置初始密码为hadoop-Replace Existing Linux System-Write changes to disk-开始安装。

5、安装完以后，会提醒你要重启一下，就是reboot，reboot就可以了。



**注意**

Centos6的镜像已经不维护了，所以使用Centos7



#### 网络配置

1、先临时性设置虚拟机ip地址：ifconfig eth0 192.168.1.110，在/etc/hosts文件中配置本地ip到host的映射

2、配置windows主机上的hosts文件：C:\Windows\System32\drivers\etc\hosts，192.168.1.110 sparkproject1

3、使用SecureCRT从windows上连接虚拟机

4、永久性配置CentOS网络

vi /etc/sysconfig/network-scripts/ifcfg-enp0s3

DEVICE=eth0

TYPE=Ethernet

ONBOOT=yes

BOOTPROTO=static

IPADDR=192.168.1.110

NETMASK=255.255.255.0

GATEWAY=192.168.1.1

5、重启网卡

service network restart



**ifconfig命令用不了**

1 编辑文件/etc/sysconfig/network-scripts/ifcfg-enp0s3,将文件中ONBOOT=no修改为ONBOOT=yes

2 重启网络服务:	service network restart

3 使用yum provieds命令查找ifconfig命令对应的软件包:	yum provides ifconfig

4 运行如下命令安装net-tools:	yum install net-tools





#### 关闭防火墙

Centos6

```
**临时关闭防火墙**
service iptables stop
service ip6tables stop

**查看防火墙的状态**
service iptables status
service ip6tables status

**永久关闭防火墙**
chkconfig iptables off
chkconfig ip6tablesoff

**查看防火墙状态**
chkconfig iptables --list

vi /etc/selinux/config
SELINUX=disabled
```



Centos7

```
CentOS 7.0默认使用的是firewall作为防火墙

查看防火墙状态
firewall-cmd --state

停止firewall
systemctl stop firewalld.service

禁止firewall开机启动
systemctl disable firewalld.service 

```





**注意**

在win7的控制面板中，关闭windows的防火墙！如果不关闭防火墙的话，就怕，可能win7和虚拟机直接无法ping通！



#### 配置DNS服务器、替换repo文件、配置yum

1、配置DNS服务器

vi /etc/resolv.conf

nameserver 61.139.2.69

nameserver 202.103.224.68
nameserver 202.103.225.68



ping www.baidu.com



1、修改repo

将CentOS7-Base-163.repo上传到CentOS中的/usr/local目录下

cd /etc/yum.repos.d/

rm -rf *

mv 自己的repo文件移动到/etc/yum.repos.d/目录中：cp /usr/local/CentOS6-Base-163.repo .

修改repo文件，把所有gpgcheck属性修改为0



**注意**

Centos6已经不维护了，所以使用Centos7



2、配置yum

yum clean all

yum makecache

yum install telnet



#### 解决无法访问外网的问题

即使更换了ip地址，重启网卡，可能还是联不通网。那么可以先将IPADDR、NETMASK、GATEWAY给删除，将BOOTPROTO改成dhcp。然后用service network restart重启网卡。此时linux会自动给分配一个ip地址，用ifconfig查看分配的ip地址。然后再次按照之前说的，配置网卡，将ip改成自动分配的ip地址。最后再重启一次网卡。



### 安装jdk7

1、将jdk-7u80-linux-x64.rpm上传到虚拟机中

2、安装JDK：rpm -ivh jdk-7u80-linux-x64.rpm

3、配置jdk相关的环境变量

vi  ~/.bashrc

export JAVA_HOME=/usr/java/latest

export PATH=$PATH:$JAVA_HOME/bin

source ~/.bashrc

4、测试jdk安装是否成功：java -version

5、rm -f /etc/udev/rules.d/70-persistent-net.rules





**原因**：
64位系统中安装了32位程序。

**解决方法**：
运行以下命令：
[root@test ~]# yum install libgcc.i686

[root@test ~]# yum install ld-linux.so.2

再重新安装JDK



### 安装第二台和第三台虚拟机

1、安装上述步骤，再安装两台一模一样环境的虚拟机。

2、另外两台机器的hostname分别设置为sparkproject2和sparkproject3即可

3、在安装的时候，另外两台虚拟机的centos镜像文件必须重新拷贝一份，放在新的目录里，使用各自自己的镜像文件。

4、虚拟机的硬盘文件也必须重新选择一个新的目录，以更好的区分。

5、安装好之后，记得要在三台机器的/etc/hosts文件中，配置全三台机器的ip地址到hostname的映射，而不能只配置本机，这个很重要！

6、在windows的hosts文件中也要配置全三台机器的ip地址到hostname的映射。



### 配置集群ssh免密码登录

1、在三台机器的/etc/hosts文件中，都配置对三台机器的ip-hostname的映射

2、首先在三台机器上配置对本机的ssh免密码登录

生成本机的公钥，过程中不断敲回车即可，ssh-keygen命令默认会将公钥放在/root/.ssh目录下

ssh-keygen -t rsa

将公钥复制为authorized_keys文件，此时使用ssh连接本机就不需要输入密码了

cd /root/.ssh

cp id_rsa.pub authorized_keys

3、接着配置三台机器互相之间的ssh免密码登录

使用ssh-copy-id -i spark命令将本机的公钥拷贝到指定机器的authorized_keys文件中（方便好用）



host文件：

192.168.1.115 sparkproject1
192.168.1.117 sparkproject2
192.168.1.118 sparkproject3



### hadoop集群搭建

#### 安装hadoop包

1、使用hadoop-2.5.0-cdh5.3.6.tar.gz，上传到虚拟机的/usr/local目录下。（http://archive.cloudera.com/cdh5/cdh/5/）

2、将hadoop包进行解压缩：tar -zxvf hadoop-2.5.0-cdh5.3.6.tar.gz

3、对hadoop目录进行重命名：mv hadoop-2.5.0-cdh5.3.6 hadoop

4、配置hadoop相关环境变量

vi ~/.bashrc

export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin

source ~/.bashrc

5、创建/usr/local/data目录



#### 修改core-site.xml

 	<property>
 	    <name>fs.default.name</name>
 	    <value>hdfs://sparkproject1:9000</value>
 	</property>



#### 修改hdfs-site.xml

```
<property>

     <name>dfs.name.dir</name>

     <value>/usr/local/data/namenode</value>

</property>

<property>

     <name>dfs.data.dir</name>

     <value>/usr/local/data/datanode</value>

</property>

<property>

     <name>dfs.tmp.dir</name>

     <value>/usr/local/data/tmp</value>

</property>

<property>

     <name>dfs.replication</name>

     <value>2</value>

</property>
```







#### 修改mapred-site.xml

```
<property>

 <name>mapreduce.framework.name</name>

 <value>yarn</value>

</property>
```



#### 修改yarn-site.xml

```
<property>

 <name>yarn.resourcemanager.hostname</name>

 <value>sparkproject1</value>

</property>

<property>

 <name>yarn.nodemanager.aux-services</name>

 <value>mapreduce_shuffle</value>

</property>

```



#### 修改slaves文件

```
sparkproject2
sparkproject3
```



#### 在另外两台机器上搭建hadoop

1、使用如上配置在另外两台机器上搭建hadoop，可以使用scp命令将sparkproject1上面的hadoop安装包和~/.bashrc配置文件都拷贝过去。（scp -r hadoop root@sparkproject2:/usr/local）

2、要记得对.bashrc文件进行source，以让它生效。

3、记得在sparkproject2和sparkproject3的/usr/local目录下创建data目录。



#### 启动hdfs集群

1、格式化namenode：在sparkproject1上执行以下命令，hdfs namenode -format

2、启动hdfs集群：start-dfs.sh
 3、验证启动是否成功：jps、50070端口

sparkproject1：namenode、secondarynamenode

sparkproject2：datanode

sparkproject3：datanode

4、hdfs dfs -put hello.txt /hello.txt



#### 启动yarn集群

1、启动yarn集群：start-yarn.sh
 2、验证启动是否成功：jps、8088端口

sparkproject1：resourcemanager、nodemanager

sparkproject2：nodemanager

sparkproject3：nodemanager



### hive的安装

#### hive包安装

1、将课程提供的hive-0.13.1-cdh5.3.6.tar.gz使用WinSCP上传到sparkproject1的/usr/local目录下。

2、解压缩hive安装包：tar -zxvf hive-0.13.1-cdh5.3.6.tar.gz

3、重命名hive目录：mv hive-0.13.1-cdh5.3.6 hive

4、配置hive相关的环境变量

vi ~/.bashrc

export HIVE_HOME=/usr/local/hive

export PATH=$HIVE_HOME/bin

source ~/.bashrc



#### 安装mysql



1、在sparkproject1上安装mysql。

2、使用yum安装mysql server。

yum install -y mysql-server

service mysqld start

chkconfig mysqld on

3、使用yum安装mysql connector

yum install -y mysql-connector-java

4、将mysql connector拷贝到hive的lib包中

cp /usr/share/java/mysql-connector-java-5.1.17.jar /usr/local/hive/lib

5、在mysql上创建hive元数据库，创建hive账号，并进行授权

create database if not exists hive_metadata;

grant all privileges on hive_metadata.* to 'hive'@'%' identified by 'hive';

grant all privileges on hive_metadata.* to 'hive'@'localhost' identified by 'hive';

grant all privileges on hive_metadata.* to 'hive'@'sparkproject1' identified by 'hive';

flush privileges;

use hive_metadata;



版本：mysql-community-libs     x86_64  5.6.51-2.el7 

Server version: 5.6.51 MySQL Community Server (GPL)

mysql驱动包 5.1.38



#### 配置hive-site.xml

cd /usr/local/hive/conf

mv hive-default.xml.template hive-site.xml

```


<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:mysql://sparkproject1:3306/hive_metadata?createDatabaseIfNotExist=true</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionDriverName</name>
  <value>com.mysql.jdbc.Driver</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionUserName</name>
  <value>hive</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionPassword</name>
  <value>hive</value>
</property>

```



#### 配置hive-env.sh和hive-config.sh

mv hive-env.sh.template hive-env.sh

```
vi /usr/local/hive/bin/hive-config.sh
export JAVA_HOME=/usr/java/latest
export HIVE_HOME=/usr/local/hive
export HADOOP_HOME=/usr/local/hadoop

```



### Zookeeper 集群的搭建

#### zookeeper包的安装

1、将课程提供的zookeeper-3.4.5-cdh5.3.6.tar.gz使用WinSCP拷贝到sparkproject1的/usr/local目录下。

2、对zookeeper-3.4.5-cdh5.3.6.tar.gz进行解压缩：tar -zxvf zookeeper-3.4.5-cdh5.3.6.tar.gz。

3、对zookeeper目录进行重命名：mv zookeeper-3.4.5-cdh5.3.6 zk。

4、配置zookeeper相关的环境变量

vi ~/.bashrc

export ZOOKEEPER_HOME=/usr/local/zk

export PATH=$PATH:$ZOOKEEPER_HOME/bin

source ~/.bashrc



####  配置zoo.cfg

cd zk/conf

mv zoo_sample.cfg zoo.cfg



vi zoo.cfg

修改：dataDir=/usr/local/zk/data

新增：

server.0=sparkproject1:2888:3888 

server.1=sparkproject2:2888:3888

server.2=sparkproject3:2888:3888



#### 设置zk节点标识

cd zk

mkdir data

cd data



vi myid

0



#### 搭建zk集群

1、在另外两个节点上按照上述步骤配置ZooKeeper，使用scp将zk和.bashrc拷贝到spark2和spark3上即可。

2、唯一的区别是spark2和spark3的标识号分别设置为1和2。



#### 启动ZooKeeper集群

1、分别在三台机器上执行：zkServer.sh start。

2、检查ZooKeeper状态：zkServer.sh status，应该是一个leader，两个follower

3、jps：检查三个节点是否都有QuromPeerMain进程。



### kafka集群的搭建

#### scala的安装

1、将课程提供的scala-2.11.4.tgz使用WinSCP拷贝到sparkproject1的/usr/local目录下。

2、对scala-2.11.4.tgz进行解压缩：tar -zxvf scala-2.11.4.tgz。

3、对scala目录进行重命名：mv scala-2.11.4 scala

4、配置scala相关的环境变量

vi ~/.bashrc

export SCALA_HOME=/usr/local/scala

export PATH=$PATH:$SCALA_HOME/bin

source ~/.bashrc

5、查看scala是否安装成功：scala -version

6、按照上述步骤在sparkproject2和sparkproject3机器上都安装好scala。使用scp将scala和.bashrc拷贝到另外两台机器上即可。



#### 安装Kafka包

1、将课程提供的kafka_2.9.2-0.8.1.tgz使用WinSCP拷贝到sparkproject1的/usr/local目录下。

2、对kafka_2.9.2-0.8.1.tgz进行解压缩：tar -zxvf kafka_2.9.2-0.8.1.tgz。

3、对kafka目录进行改名：mv kafka_2.9.2-0.8.1 kafka

4、配置kafka

vi /usr/local/kafka/config/server.properties

broker.id：依次增长的整数，0、1、2，集群中Broker的唯一id

zookeeper.connect=192.168.1.115:2181,192.168.1.117:2181,192.168.1.118:2181

5、安装slf4j

将课程提供的slf4j-1.7.6.zip上传到/usr/local目录下

unzip slf4j-1.7.6.zip

把slf4j中的slf4j-nop-1.7.6.jar复制到kafka的libs目录下面



####  搭建kafka集群

1、按照上述步骤在另外两台机器分别安装kafka。用scp把kafka拷贝到sparkproject2和sparkproject3即可。

2、唯一区别的，就是server.properties中的broker.id，要设置为1和2



#### 启动kafka集群

1、解决kafka Unrecognized VM option 'UseCompressedOops'问题

vi /usr/local/kafka/bin/kafka-run-class.sh

if [ -z "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then

 KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true"

fi

去掉-XX:+UseCompressedOops即可



2、在三台机器上的kafka目录下，分别执行以下命令：nohup bin/kafka-server-start.sh config/server.properties &



3、使用jps检查启动是否成功



#### 测试kafka集群

使用基本命令检查kafka是否搭建成功



bin/kafka-topics.sh --zookeeper 192.168.1.115:2181,192.168.1.117:2181,192.168.1.118:2181 --topic TestTopic --replication-factor 1 --partitions 1 --create



bin/kafka-console-producer.sh --broker-list 192.168.1.115:9092,192.168.1.117:9092,192.168.1.118:9092 --topic TestTopic



bin/kafka-console-consumer.sh --zookeeper 192.168.1.115:2181,192.168.1.117:2181,192.168.1.118:2181 --topic TestTopic --from-beginning



### flume安装

#### 安装flume

1、将课程提供的flume-ng-1.5.0-cdh5.3.6.tar.gz使用WinSCP拷贝到sparkproject1的/usr/local目录下。

2、对flume进行解压缩：tar -zxvf flume-ng-1.5.0-cdh5.3.6.tar.gz

3、对flume目录进行重命名：mv apache-flume-1.5.0-cdh5.3.6-bin flume

4、配置scala相关的环境变量

vi ~/.bashrc

export FLUME_HOME=/usr/local/flume

export FLUME_CONF_DIR=$FLUME_HOME/conf

export PATH=$PATH:$FLUME_HOME/bin

source ~/.bashrc



#### 修改flume配置文件

vi /usr/local/flume/conf/flume-conf.properties



\#agent1表示代理名称

agent1.sources=source1

agent1.sinks=sink1

agent1.channels=channel1

\#配置source1

agent1.sources.source1.type=spooldir

agent1.sources.source1.spoolDir=/usr/local/logs

agent1.sources.source1.channels=channel1

agent1.sources.source1.fileHeader = false

agent1.sources.source1.interceptors = i1

agent1.sources.source1.interceptors.i1.type = timestamp

\#配置channel1

agent1.channels.channel1.type=file

agent1.channels.channel1.checkpointDir=/usr/local/logs_tmp_cp

agent1.channels.channel1.dataDirs=/usr/local/logs_tmp

\#配置sink1

agent1.sinks.sink1.type=hdfs

agent1.sinks.sink1.hdfs.path=hdfs://sparkproject1:9000/logs

agent1.sinks.sink1.hdfs.fileType=DataStream

agent1.sinks.sink1.hdfs.writeFormat=TEXT

agent1.sinks.sink1.hdfs.rollInterval=1

agent1.sinks.sink1.channel=channel1

agent1.sinks.sink1.hdfs.filePrefix=%Y-%m-%d



#### 创建需要的文件夹

本地文件夹：mkdir /usr/local/logs

HDFS文件夹：hdfs dfs -mkdir /logs



#### 启动flume-agent

flume-ng agent -n agent1 -c conf -f /usr/local/flume/conf/flume-conf.properties -Dflume.root.logger=DEBUG,console



#### 测试flume

新建一份文件，移动到/usr/local/logs目录下，flume就会自动上传到HDFS的/logs目录中



### Spark的环境配置

#### 安装spark客户端

1、将spark-1.5.1-bin-hadoop2.4.tgz使用WinSCP上传到/usr/local目录下。

2、解压缩spark包：tar -zxvf spark-1.5.1-bin-hadoop2.4.tgz。

3、重命名spark目录：mv spark-1.5.1-bin-hadoop2.4 spark

4、修改spark环境变量

vi ~/.bashrc

export SPARK_HOME=/usr/local/spark

export CLASSPATH=.:$CLASSPATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib

export PATH=$PATH:$SPARK_HOME/bin

source ~/.bashrc



#### 修改spark-env.sh文件

1、cd /usr/local/spark/conf

2、cp spark-env.sh.template spark-env.sh

3、vi spark-env.sh

export JAVA_HOME=/usr/java/latest

export SCALA_HOME=/usr/local/scala

export HADOOP_HOME=/usr/local/hadoop

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop



#### 用yarn-client模式提交spark作业

/usr/local/spark/bin/spark-submit \

--class org.apache.spark.example.JavaSparkPi \

--master yarn-client \

--num-executors 1 \

--driver-memory 10m \

--executor-memory 10m \

--executor-cores 1 \

/usr/local/spark/lib/spark-examples-1.5.1-hadoop2.4.0.jar \



### 脚本文件

统一放在 sparkproject1机器下 /usr/local/bin 目录下



更改文件权限 chmod 777 文件

#### show-jps.sh （查看进程脚本）

```shell
#!/bin/bash
for((host=1; host<=3; host++)); do
                echo "================  sparkproject$host JPS  ====================="
        ssh sparkproject$host "jps"
done
```



#### xsync （同步脚本）

```shell
#!/bin/bash
#1 获取输入参数个数，如果没有参数，直接退出
pcount=$#
if ((pcount==0)); then
echo no args;
exit;
fi
 
#2 获取文件名称
p1=$1
fname=`basename $p1`
echo fname=$fname
 
#3 获取上级目录到绝对路径
pdir=`cd -P $(dirname $p1); pwd`
echo pdir=$pdir
 
#4 获取当前用户名称
user=`whoami`
 
#5 循环
for((host=1; host<4; host++)); do
        echo ------------------- sparkproject$host --------------
        rsync -rvl $pdir/$fname $user@sparkproject$host:$pdir
done

```



#### hadoop集群启动

```shell
#!/bin/bash
user=`whoami`

echo "===============     开始启动所有节点服务        ==============="
for((host=1; host<=3; host++)); 
do
                echo "--------------- sparkproject$host Zookeeper...... ----------------"
        ssh $user@sparkproject$host 'zkServer.sh start'
done

echo "================    正在启动HDFS                ==============="
ssh $user@sparkproject1 'start-dfs.sh'

echo "================    正在启动YARN                ==============="
ssh $user@sparkproject1 'start-yarn.sh'

echo "================    正在启动kafka                ==============="
for((i=1;i<4;i++))
do
        echo ----------sparkproject$i----------
        ssh $user@sparkproject$i '/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties'
        echo sparkproject$i start successfully
done

```



#### hadoop集群关闭

```shell
#!/bin/bash
user=`whoami`
echo "================     开始停止所有节点服务        ==============="

echo "================    正在停止kafka                ==============="
for((i=1;i<4;i++))
do
        echo ----------sparkproject$i----------
        ssh $user@sparkproject$i '/usr/local/kafka/bin/kafka-server-stop.sh'
        echo sparkproject$i stop successfully
done

echo "================    正在停止YARN                ==============="
ssh $user@sparkproject1 'stop-yarn.sh'

echo "================    正在停止HDFS                ==============="
ssh $user@sparkproject1 'stop-dfs.sh'



echo "===============     正在停止Zookeeper......     ==============="
for((host=1; host<=3; host++)); do
        echo "--------------- sparkproject$host Zookeeper...... ----------------"
        ssh $user@sparkproject$host 'zkServer.sh stop'
done
```



#### zookeeper集群启动脚本

```shell
#!/bin/bash
for((i=102;i<105;i++))
do
echo ----------hadoop$i----------
        ssh chenli@hadoop$i 'source /etc/profile && /opt/module/zookeeper-3.4.10/bin/zkServer.sh start'
done

```



#### zookeeper集群关闭脚本

```shell
#!/bin/bash
for((i=102;i<105;i++))
do
        echo ----------hadoop$i----------
        ssh chenli@hadoop$i 'source /etc/profile && /opt/module/zookeeper-3.4.10/bin/zkServer.sh stop'
done

```



#### kafka集群启动脚本

```shell
#!/bin/bash
for((i=102;i<105;i++))
do
        echo ----------hadoop$i----------
        ssh chenli@hadoop$i 'source /etc/profile && /opt/module/kafka-2.11/bin/kafka-server-start.sh -daemon /opt/module/kafka-2.11/config/server.properties'
        echo hadoop$i start successfully
done

```



#### kafka集群关闭脚本

```shell
#!/bin/bash
for((i=102;i<105;i++))
do
        echo ----------hadoop$i----------
        ssh chenli@hadoop$i 'source /etc/profile && /opt/module/kafka-2.11/bin/kafka-server-stop.sh'
        echo hadoop$i stop successfully
done

```







## 记录小坑

### CentOS报错base ls command not found

主要原因是因为环境变量的问题，编辑profile文件、~/.bashrc文件没有写正确，导致在命令行下 ls等命令不能够识别。

解决办法：在命令行下打入下面这段就可以了 
**export PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin**



### Centos7版本对防火墙进行 加强,不再使用原来的iptables,启用firewall

```
1.查看已开放的端口(默认不开放任何端口)
firewall-cmd --list-ports
2.开启80端口
firewall-cmd --zone=public(作用域) --add-port=80/tcp(端口和访问类型) --permanent(永久生效)
3.重启防火墙
firewall-cmd --reload
4.停止防火墙
systemctl stop firewalld.service
5.禁止防火墙开机启动
systemctl disable firewalld.service
6.删除
firewall-cmd --zone= public --remove-port=80/tcp --permanent
```



### 50070端口访问不了

记得关闭防火墙



### haoop使用上传命令报错：java.net.NoRouteToHostException: No route to host

记得关闭slaves文件的主机的防火墙。你只关闭了namenode主机的防火墙，所以你可以访问50070端口了。但你datanode主机的防火墙你没关闭，所以导致上传不了文件



### centos 7 安装mysql-server报错No package mysql-server available

在centos7中要安装mysql-server,必须先添加mysql社区repo通过输入命令：

sudo rpm -Uvh http://dev.mysql.com/get/mysql-community-release-el7-5.noarch.rpm



待成功添加完毕后重新使用yum install mysql-server命令安装mysql-server服务即可安装成功



### Centos7服务命令

systemctl命令，而不是service命令



### mysql驱动包一定要和mysql版本对应上不然会报很多错误



## 开发流程

### 一、数据调研（基础数据的结构以及架构的设计）

### 二、需求分析

### 三、技术方案的设计

### 四、数据设计

### 五、编码阶段

### 六、测试阶段

### 七、调优阶段



## 开发Spark经验准则

1、尽量少生成RDD
2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
     shuffle操作，会导致大量的磁盘读写，严重降低性能
     有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
     有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
4、无论做什么功能，性能第一

- 在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
  程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）

- 在大数据项目中，比如MapReduce、Hive、Spark、Storm，性能的重要程度，远远大于一些代码 

  的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
  主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
  如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
  此时，对于用户体验，简直就是一场灾难

  所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合



​     

​		

​	 







