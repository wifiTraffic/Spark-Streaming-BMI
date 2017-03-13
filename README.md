# Spark-Streaming-BMI
Spark Streaming 流式计算在人群BMI指数统计上的应用

Spark框架由scala编写，支持scala，java，python编程语言。
Spark分布式计算，核心的数据结构是RDD（弹性分布式数据集）
Spark Streaming支持流式计算，适用于实时大数据流的分析
Spark Streaming的核心数据结构是DStream，DStream由连续的多个时间段的RDD组成，通俗点说就是RDD的流。

BMI指数反映一个人的身高体重协调程度，BMI=体重（kg）/身高的平方（m^2）
中国BMI分级标准：BMI<=18.4为偏瘦，18.5-23.9为正常，24.0-27.9为过重，>=28.0为肥胖

模拟外界源源不断的传进人的身高体重数据：现在通过localhost的端口9999，输入每个人身高（cm）与体重（kg），用逗号隔开，一个人占一行。（数据中包含格式错误的无效数据）

每10秒统计一次已经输入的数据中各个BMI分类的人数并输出

