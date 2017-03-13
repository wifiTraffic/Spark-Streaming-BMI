import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming._

class bmi extends Serializable{
  def health(line:String): String ={
    var factor=line.split(",")
    if(factor.length!=2)return "无效数据"
    try{
      var higth=factor(0).toDouble
      var weigth=factor(1).toDouble
      var BMI=weigth/((0.01*higth)*(0.01*higth))
      if(BMI<18.4)return "偏瘦"
      else if(BMI<23.9)return "正常"
      else if(BMI<27.9)return "过重"
      else return "肥胖"
    }catch {
      case ex:Exception=>{
        return "无效数据"
      }
    }
  }
}

object ClassifiedCount {
  def main(args: Array[String]): Unit = {
    //函数字面量，输入的当前值与前一次的状态结果进行累加
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    //输入类型为K,V,S,返回值类型为K,S
    //V对应为带求和的值，S为前一次的状态
    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }
    //创建StreamingContext
    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //当前目录为checkpoint结果目录，后面会讲checkpoint在Spark Streaming中的应用
    ssc.checkpoint(".")

    //RDD的初始化结果
    val initialRDD = ssc.sparkContext.parallelize(List(("偏瘦", 0), ("正常", 0),("过重",0),("肥胖",0)))
    //使用Socket作为输入源，本例ip为localhost，端口为9999
    val lines = ssc.socketTextStream("localhost", 9999)
    //map操作
    val healthDstream = lines.map(x => (new bmi().health(x), 1))

    //updateStateByKey函数使用
    val stateDstream = healthDstream.updateStateByKey[Int](newUpdateFunc, new HashPartitioner (ssc.sparkContext.defaultParallelism), true, initialRDD)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
