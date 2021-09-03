# Spark笔记

## RDD基本概念：

RDD的本质：在并行计算的过程中实现数据的共享，这种共享就是RDD的本质。

每个RDD的计算都是以partition(分区)为单位



##  Spark基本工作流程：

 ![Spark基本框图](https://github.com/ChocolateLi/spark-project/blob/master/spark_picture/Spark%E5%9F%BA%E6%9C%AC%E6%A1%86%E6%9E%B6%E5%9B%BE.png)

1. Spark的应用分为任务调度和任务执行两个部分
2. 所以Spark程序都离不开SparkContext和Executor两部分，Executor负责执行任务，运行Executor的机器称为Worker节点，SparkContext由用户程序启动，通过资源调度模块和Executor通信。SparkContext和Executor这两部分在各种运行模式上是公用的。
3. SparkContext是程序运行的总入口，在SparkContext的初始化过程中，会分别创建DAGScheduler作业调度和TaskScheduler任务调度两个级别的调度模块
4. 作业调度模块和具体的运行模式无关，它是根据shuffle来划分调度阶段，每个阶段会构建出具体的任务，然后以TaskSets(任务组)的形式提交给任务调度模块来具体执行
5. 不同运行模式的区别主要体现在任务调度模块，任务调度模块负责启动任务、监控任务和汇报任务的情况。



## 作业调度

### 作业调度关系图

![作业调度关系图](https://github.com/ChocolateLi/spark-project/blob/master/spark_picture/Spark%E4%BD%9C%E4%B8%9A%E5%85%B3%E7%B3%BB%E5%9B%BE.png)



- Application(应用程序)：Spark应用程序由一个或多个作业组成
- Job(作业)：由一个RDD Action 生成一个或多个调度阶段所组成的一次计算作业
- Stage(调度阶段)：一个任务集多对应的调度阶段。Stage的划分是根据宽依赖(shuffle操作)来划分的
- TaskSet(任务集)：由一组关联的，但互相之间没有shuffle依赖关系的任务所组成的任务集
- Task(任务)：单个分区数据集上的最小处理流程单元



### 作业调度具体流程

![作业调度具体流程](https://github.com/ChocolateLi/spark-project/blob/master/spark_picture/Spark%E4%BD%9C%E4%B8%9A%E8%B0%83%E5%BA%A6%E6%B5%81%E7%A8%8B%E5%9B%BE.png)

1. 用户提交程序(Application)创建SparkContext实例，SparkContext根据RDD对象生成DAG图，将作业(Job)提交给DAGScheduler
2. DAGScheduler将作业(Job)划分成不同的Stage(从末端RDD开始，根据shuffle来划分)，每个Stage都是任务的集(TaskSet)，以TaskSet为单位提交给TaskScheduler
3. TaskScheduler管理任务(Task)，并通过资源管理器(Cluster Manager)[standalone模式下是Master，yarn模式下是ResourceManager]把任务(task)发给集群中的Worker的Executor
4. Worker接收到任务(Task)，启动Executor进程中的线程Task来执行任务（实际任务的运行最终由Executor类来执行，Executor对每一个任务创建一个TaskRunner类，交给线程池运行。）













