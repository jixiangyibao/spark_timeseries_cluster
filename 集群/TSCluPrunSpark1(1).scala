
/**
  * spark版本1
  */

import scala.collection.mutable.Map
import org.apache.spark.{SparkConf, SparkContext}
object TSCluPrunSpark1 {
  val inf = Double.PositiveInfinity
  val minusInf =Double.NegativeInfinity
  def main(args: Array[String]): Unit = {
    //  DensityPeaks("C:\\可以删除\\data.csv")
  // DensityPeaks("hdfs://master:9000/user/master/DATA/data7K.csv")
    DensityPeaks("hdfs://master:9000/user/master/DATA/MALLAT.csv")
  }
  def DensityPeaks(filePath: String) = {
  //  val conf = new SparkConf().setAppName("cluster").setMaster("local")
    val conf = new SparkConf().setAppName("cluster")
    val sc = new SparkContext(conf)
    val dataRdd = sc.textFile(filePath).map(x => x.split(",").map(x => x.toDouble))
    dataRdd.cache()
    val data = dataRdd.collect()
    val bcData = sc.broadcast(data)
    //阈值dc
    val dc=4
    //聚类数
    val k = 8
    //样本数
    val num = dataRdd.count().toInt
    //样本维度
    val dimension = dataRdd.first().size
    //LB_keogh的参数r
    val r = (dimension * 0.08).toInt

    var DTWCnt=0

    val begin = System.currentTimeMillis()
    //本地计算欧式距离
    val UBMat = calEDMat(data, num, dimension)
    println("计算欧氏距离所用的时间是:" + (System.currentTimeMillis() - begin))

    val begin1 = System.currentTimeMillis()
    //本地计算下界距离
    val LBMat = calLBKeoghMat(data, num, dimension, r)
    println("计算LB_Keogh距离所用的时间是:" + (System.currentTimeMillis() - begin1))

    //将UBMatrix和LBMatrix设置为广播变量
    val bcUBMat=sc.broadcast(UBMat)
    val bcLBMat=sc.broadcast(LBMat)
    val bcDimension=sc.broadcast(dimension)
    //计算局部密度
    //1.分布式计算出所有需要计算的DTW
    val begin2 = System.currentTimeMillis()
    val idRdd = sc.parallelize(0 to num - 1)
    val cartIdRdd = idRdd.cartesian(idRdd).filter(pair => pair._1 < pair._2)
    val dtwDistanceRdd = cartIdRdd.map {
      case (i, j) => {
        val bcdata = bcData.value
        val bcubmat=bcUBMat.value
        val bclbmat=bcLBMat.value
        val bcdimension=bcDimension.value
        var disTmp = 0.0
        if (bclbmat(i)(j) <= dc && bcubmat(i)(j) > dc) {
          //需要计算DTW
          val xs = bcdata(i)
          val ys = bcdata(j)
          val DTWMatrix = Array.ofDim[Double](bcdimension, bcdimension)
          for (i <- 0 until bcdimension) {
            for (j <- 0 until bcdimension) {
              DTWMatrix(i)(j) = Math.pow((xs(i) - ys(j)), 2)
            }
          }
          for (i <- 1 until bcdimension) {
            DTWMatrix(0)(i) = DTWMatrix(0)(i - 1) + DTWMatrix(0)(i)
          }
          for (i <- 1 until bcdimension) {
            DTWMatrix(i)(0) = DTWMatrix(i - 1)(0) + DTWMatrix(i)(0)
          }
          //计算接下来的行和列
          for (i <- 1 until bcdimension) {
            for (j <- 1 until bcdimension) {
              val d1 = DTWMatrix(i)(j - 1) + DTWMatrix(i)(j)
              val d2 = DTWMatrix(i - 1)(j) + DTWMatrix(i)(j)
              val d3 = DTWMatrix(i - 1)(j - 1) + DTWMatrix(i)(j)
              var minDist = Math.min(d1, d2)
              minDist = Math.min(minDist, d3)
              DTWMatrix(i)(j) = minDist
            }
          }
          disTmp = Math.sqrt(DTWMatrix(bcdimension - 1)(bcdimension - 1))
        }
        (i,j,disTmp)
      }

    }
    val dtwDistance = dtwDistanceRdd.collect()

    //DTW距离的稀疏矩阵
    val Dsparse = Array.ofDim[Double](num, num)
    for(i<- dtwDistance){
      if(i._3!=0){
        Dsparse(i._1)(i._2)=i._3
        DTWCnt+=1
      }
    }
    for (i <- 0 until num) {
      for (j <- 0 until i) {
        Dsparse(i)(j) = Dsparse(j)(i)
      }
    }
    //2.开始本地计算局部密度
    val localDensity = Array.ofDim[Double](num)
    for (i <- 0 until num) {
      var neighborNum = 0
      for (j <- 0 until num) {
        if (i == j) {}
        else if(LBMat(i)(j)<=dc && UBMat(i)(j) > dc){
          if (Dsparse(i)(j) <= dc) {
            neighborNum += 1
          }
        }
        else if (UBMat(i)(j) < dc) {
          neighborNum += 1
        }
      }

      localDensity(i) = neighborNum
    }
    val localDensityRdd = sc.makeRDD(localDensity).zipWithIndex()
    //localDensitySorted:降序排列的局部密度(index,id,neighborNum)
    val localDensitySortedRdd = localDensityRdd.sortBy(-_._1).zipWithIndex().map {
      case ((neighborNum, id), index) => {
        (index, id, neighborNum)
      }
    }
    val localDensitySorted = localDensitySortedRdd.collect()
    println("计算局部密度所用的时间是:" + (System.currentTimeMillis() - begin2))
    /*println("排序后的局部密度值:")
    for(i<-localDensitySorted)
      println(i._1+" "+i._2+" "+i._3)
*/
    //计算与更高密度点的距离
    //1.本地计算上界
    val ub = Array.ofDim[Double](num)
    for (i <- 0 until num) {
      //此点的id
      val id_i = localDensitySorted(i)._2.toInt
      ub(id_i) = inf
      //所有比i密度高的点j
      for (j <- 0 until i) {
        //此点的id
        val id_j = localDensitySorted(j)._2.toInt
        //i,j之间的距离已经计算出来了
        if (Dsparse(id_i)(id_j) != 0) {
          var tmp=Dsparse(id_i)(id_j)
          if(Dsparse(id_i)(id_j)<LBMat(id_i)(id_j)){
            tmp=LBMat(id_i)(id_j)
          }
          if (ub(id_i) > tmp) {
            ub(id_i) = tmp
          }
        }
        //i,j之间的距离尚未计算
        else {
          if (ub(id_i) > UBMat(id_i)(id_j)) {
            ub(id_i) = UBMat(id_i)(id_j)
          }
        }
      }
    }
    val bcLocalDensitySorted=sc.broadcast(localDensitySorted)
    val bcUb=sc.broadcast(ub)
    val bcDsparse=sc.broadcast(Dsparse)
    //2.开始计算与更高密度点的距离
    //2.1分布式计算DTW
    val index1Rdd=sc.parallelize(1 to (num-1))
    val cartIndex1=index1Rdd.cartesian(idRdd).filter(pair=>pair._1>pair._2)
    val dtwDistanceRdd2=cartIndex1.map{
     case(i,j)=>{
       val bclocalDensitySorted=bcLocalDensitySorted.value
       val bclbmat=bcLBMat.value
       val bcub=bcUb.value
       val bcdsparse=bcDsparse.value
       val bcdata = bcData.value
       val bcdimension=bcDimension.value

       val id_i = bclocalDensitySorted(i)._2.toInt
       val id_j = bclocalDensitySorted(j)._2.toInt
       var distance=0.0
       if (bclbmat(id_i)(id_j) <= bcub(id_i)){
         if (bcdsparse(id_i)(id_j) == 0){
           val xs=bcdata(id_i)
           val ys=bcdata(id_j)
           val DTWMatrix = Array.ofDim[Double](bcdimension, bcdimension)
           for (i <- 0 until bcdimension) {
             for (j <- 0 until bcdimension) {
               DTWMatrix(i)(j) = Math.pow((xs(i) - ys(j)), 2)
             }
           }
           for (i <- 1 until bcdimension) {
             DTWMatrix(0)(i) = DTWMatrix(0)(i - 1) + DTWMatrix(0)(i)
           }
           for (i <- 1 until bcdimension) {
             DTWMatrix(i)(0) = DTWMatrix(i - 1)(0) + DTWMatrix(i)(0)
           }
           //计算接下来的行和列
           for (i <- 1 until bcdimension) {
             for (j <- 1 until bcdimension) {
               val d1 = DTWMatrix(i)(j - 1) + DTWMatrix(i)(j)
               val d2 = DTWMatrix(i - 1)(j) + DTWMatrix(i)(j)
               val d3 = DTWMatrix(i - 1)(j - 1) + DTWMatrix(i)(j)
               var minDist = Math.min(d1, d2)
               minDist = Math.min(minDist, d3)
               DTWMatrix(i)(j) = minDist
             }
           }
           distance=Math.sqrt(DTWMatrix(dimension - 1)(dimension - 1))
         }

       }
       (id_i,id_j,distance)
     }
    }
    val dtw=dtwDistanceRdd2.collect()
    for(i<- dtw){
      if(i._3!=0){
        Dsparse(i._1)(i._2)=i._3
        DTWCnt+=1
      }
    }
    val perc=DTWCnt/(num*(num+1)/2.0)*100
    println("DTW计算的次数是:"+perc)
    val begin44=System.currentTimeMillis()
    val distanceToHighDensity = Array.ofDim[Double](num)
    val distanceToHighDensityId = Array.ofDim[Int](num)
    for (i <- 1 until num) {
      if(i==20) {val tmp=0}
      var minDisToI = inf
      var minDisToIId = 0
      //此点的id
      val id_i = localDensitySorted(i)._2.toInt

      //所有比i密度高的点j
      for (j <- 0 until i) {
        //此点的id
        val id_j = localDensitySorted(j)._2.toInt
        if (LBMat(id_i)(id_j) > ub(id_i)) {}
        else {
          if (minDisToI > Dsparse(id_i)(id_j)) {
            minDisToI = Dsparse(id_i)(id_j)
            minDisToIId = id_j
          }
        }
      }
      distanceToHighDensity(id_i) = minDisToI
      distanceToHighDensityId(id_i) = minDisToIId
    }
    distanceToHighDensity(localDensitySorted(0)._2.toInt)=inf
    //计算密度与距离的乘积
    val productOfDenAndDis = Array.ofDim[Double](num)
    for (i <- 0 until num) {
      productOfDenAndDis(i) = localDensity(i) * distanceToHighDensity(i)
    }
    val productOfDenAndDisSortedRdd = sc.makeRDD(productOfDenAndDis).zipWithIndex().sortBy(-_._1).zipWithIndex()
    val begin4=System.currentTimeMillis()
    //选择K个聚类中心
    val cluster = productOfDenAndDisSortedRdd.map {
      case ((_, id), index) => {
        if (index < k) {
          (id.toInt, (index + 1).toDouble)
        }
        else {
          (id.toInt, -1.0)
        }
      }
    }
    val clusterCollection = cluster.collect()
    var clusterMap: Map[Int, Int] = scala.collection.mutable.Map()
    for (i <- 0 until k) {
      val key = clusterCollection(i)._1
      val value = clusterCollection(i)._2.toInt
      clusterMap += (key -> value)
    }
    //对于剩下的数据分配标签
    for(i<-0 until num){
      // println(i)
      val id_i=localDensitySorted(i)._2.toInt
      if(!clusterMap.contains(id_i)){
        val label=clusterMap(distanceToHighDensityId(id_i))//出错
        clusterMap += (id_i -> label)
      }

    }
/*
    println("聚类结果")

    for(i<- clusterMap) {
      println(i._1 + " " + i._2)
    }

*/
    println("聚类过程所用的时间是:" + (System.currentTimeMillis() - begin4))

    println("time for cluster is:" + (System.currentTimeMillis() - begin))

  }


  def calEDMat(data: Array[Array[Double]], num: Int,dimension:Int): Array[Array[Double]] = {
    val DisMat = Array.ofDim[Double](num, num)
    for (i <- 0 until num) {
      for (j <- i until num) {
        // else if(DisMat(j)(i)!=0)
        // DisMat(i)(j)=DisMat(j)(i)
        if(i!=j) {
          val xs = data(i)
          val ys = data(j)
          var sum=0.0
          for(k<-0 until dimension){
            sum+=Math.pow((xs(k)-ys(k)),2)
          }
          sum=Math.sqrt(sum)
          DisMat(i)(j) = sum
          DisMat(j)(i) = sum
        }
      }
    }
    DisMat
  }
  def calLBKeoghMat(data: Array[Array[Double]], num: Int, dimension:Int,r: Int): Array[Array[Double]] = {
    val LBMatrix = Array.ofDim[Double](num, num)
    for (i <- 0 until num) {
      val Q = data(i)
      val U=Array.ofDim[Double](dimension)
      val L=Array.ofDim[Double](dimension)
      for (k <- 0 until dimension) {
        var st = k - r
        var ed = k + r
        if (st < 0)
          st = 0
        if (ed > dimension - 1)
          ed = dimension - 1
        var maxElem = minusInf
        var minElem = inf
        for (tmp <- st until ed) {
          if (Q(tmp) > maxElem)
            maxElem = Q(tmp)
          if(Q(tmp)<minElem)
            minElem=Q(tmp)
        }
        U(k)=maxElem
        L(k)=minElem
      }
      for(j<-0 until num){
        val C=data(j)
        for(p<-0 until dimension){
          if(C(p)>U(p))
            LBMatrix(i)(j)+=Math.pow(C(p)-U(p),2)
          else if(C(p)<L(p))
            LBMatrix(i)(j)+=Math.pow(C(p)-L(p),2)
        }
        LBMatrix(i)(j)=Math.sqrt(LBMatrix(i)(j))
      }
    }
    for(i<-0 until num)
      for(j<-1 until num){
        if(LBMatrix(i)(j)!=LBMatrix(j)(i))
        {
          if(LBMatrix(i)(j)<LBMatrix(j)(i))
            LBMatrix(i)(j)=LBMatrix(j)(i)
          else
            LBMatrix(j)(i)=LBMatrix(i)(j)
        }
      }
    LBMatrix
  }
}











