
/**
  * spark版本（运行时间长）
  */
import java.awt.Dimension
import scala.collection.mutable.Map
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter}
import scala.swing.event.ButtonClicked
import scala.swing.{Button, FileChooser, FlowPanel, Label, MainFrame, SimpleSwingApplication, TextField}
object sparkClu extends SimpleSwingApplication {
  var fileChooser = new FileChooser(new File("."))
  fileChooser.title = "fileChooser"
  val button = new Button {
    text = "choose a file from local";
    preferredSize = new Dimension(300, 100)
  }
  val label = new Label {
    text = "no any file selected yet";
    preferredSize = new Dimension(300, 100)
  }
  val textField = new TextField {
    preferredSize = new Dimension(800, 300)
  }
  val mainPanel = new FlowPanel {
    contents += button;
    contents += label;

  }
  val inf = 999999999.0
  val minusInf = -999999999.0

  def top = new MainFrame {
    preferredSize = new Dimension(1200, 800)
    contents = mainPanel
    listenTo(button)
    reactions += {
      case ButtonClicked(e) => {
        val result = fileChooser.showOpenDialog(mainPanel)
        if (result == FileChooser.Result.Approve) {
          label.text = fileChooser.selectedFile.getPath()
          DensityPeaks(label.text)
        }
      }
    }
  }
  def DensityPeaks(filePath: String) = {
    val conf = new SparkConf().setAppName("cluster").setMaster("local")
    val sc = new SparkContext(conf)
    val dataRdd = sc.textFile(filePath).map(x => x.split(",").map(x => x.toDouble))
    val data = dataRdd.collect()
    val dimension = data(0).length
    var num = 0
    for (_ <- data)
      num = num + 1
    //手动调节参数1：阈值dc
    val dc =4
    //手动调节参数2：聚类数k
    val k = 8
    //手动调节参数3：LB_keogh的r
    val r=(dimension*0.08).toInt

    val begin = System.currentTimeMillis()
    //计算DTW的上界，选择欧氏距离作为上界
    val UBMat = calEDMat(data, num,dimension)
    println("计算欧氏距离所用的时间是:" + (System.currentTimeMillis() - begin))
    val begin1=System.currentTimeMillis()
    //计算DTW的下界，选择LB_Kim距离作为下界
    //    val LBMat = calLBKimMat(data, num)
    //计算DTW的下界，选择LB_Keogh距离作为下界
    val LBMat =calLBKeoghMat(data,num,dimension,r)
    println("计算LB_Keogh距离所用的时间是:" + (System.currentTimeMillis() - begin1))

    val writer = new PrintWriter(new File("C:\\Users\\刘咏\\IdeaProjects\\wordCount\\UBMatrix.txt" ))
    writer.println("欧式距离矩阵如下：")
    for(i<-0 until num){
      for(j<-0 until num){
        writer.print(UBMat(i)(j))
        writer.print(" ")
      }
      writer.println()
    }
    writer.close()
    val writer1 = new PrintWriter(new File("C:\\Users\\刘咏\\IdeaProjects\\wordCount\\LBMatrix.txt" ))
    writer1.println("LB_keogh距离矩阵如下：")
    for(i<-0 until num){
      for(j<-0 until num){
        writer1.print(LBMat(i)(j))
        writer1.print(" ")
      }
      writer1.println()
    }
    writer1.close()

    //1.计算局部密度时剪枝
    //DTW距离的稀疏矩阵
    val Dsparse = Array.ofDim[Double](num, num)
    var DTWCnt = 0
    //localDensity:局部密度(id,neighborNum)
    //计算局部密度（剪枝版本）
    val begin2=System.currentTimeMillis()
    val localDensity = Array.ofDim[Double](num)
    for (i <- 0 until num) {
      var neighborNum = 0
      for (j <- 0 until num) {
        if (i == j) {}
        else if(LBMat(i)(j)<=dc && UBMat(i)(j) > dc){
          if (Dsparse(i)(j) == 0) {
            Dsparse(i)(j) = DTW(data(i), data(j))
            Dsparse(j)(i) = Dsparse(i)(j)
            DTWCnt += 1
            //println(DTWCnt)
          }
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
    println("到这里计算了"+DTWCnt+"次DTW")
    println("计算局部密度所用的时间是:" + (System.currentTimeMillis() - begin2))

    val localDensityRdd = sc.makeRDD(localDensity).zipWithIndex()
    //localDensitySorted:降序排列的局部密度(index,id,neighborNum)
    val localDensitySortedRdd = localDensityRdd.sortBy(-_._1).zipWithIndex().map {
      case ((neighborNum, id), index) => {
        (index, id, neighborNum)
      }
    }
    val localDensitySorted = localDensitySortedRdd.collect()
/*
  println("排序后的局部密度值:")

  //显示局部密度值
  for(i<-localDensitySorted)
    println(i._1+" "+i._2+" "+i._3)
*/
    //局部密度值降序排列的id值
    /*
    val localDensitySortedId = localDensitySortedRdd.map {
      case (_, id, _) =>
        id
    } //.collect()


    //2.计算与更高密度点的距离时剪枝

    //2.1上界计算 scala版本
*/
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
    //2.2计算与更高点的距离（剪枝）
    val begin44=System.currentTimeMillis()
    val distanceToHighDensity = Array.ofDim[Double](num)
    val distanceToHighDensityId = Array.ofDim[Int](num)
    // var maxDistanceOfHigherDensity = 0.0
    for (i <- 0 until num) {
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
          if (Dsparse(id_i)(id_j) == 0) {
            Dsparse(id_i)(id_j) = DTW(data(id_i), data(id_j))
            Dsparse(id_j)(id_i)=Dsparse(id_i)(id_j)
            DTWCnt += 1
          }
          if (minDisToI > Dsparse(id_i)(id_j)) {
            minDisToI = Dsparse(id_i)(id_j)
            minDisToIId = id_j
          }
        }
      }
      distanceToHighDensity(id_i) = minDisToI
      distanceToHighDensityId(id_i) = minDisToIId
    }
    val perc=DTWCnt/(num*(num+1)/2.0)*100
    println("DTW计算的次数是:"+perc)
/*
    val writer2 = new PrintWriter(new File("C:\\Users\\刘咏\\IdeaProjects\\wordCount\\DSparse.txt" ))
    writer2.println("DTW距离矩阵如下：")
    for(i<-0 until num){
      for(j<-0 until num){
        writer2.print(Dsparse(i)(j))
        writer2.print(" ")
      }
      writer2.println()
    }
    writer2.close()
*/
    //distanceToHighDensity(localDensitySorted(0)._2.toInt)=inf
/*
  println("与更高点的距离:")
  for(i<-0 until num){
    println(i+" "+distanceToHighDensityId(i)+" "+distanceToHighDensity(i))
  }
*/
    println("计算与更高点的距离2所用的时间是:" + (System.currentTimeMillis() - begin44))

    //计算密度与距离的乘积
    val productOfDenAndDis = Array.ofDim[Double](num)
    for (i <- 0 until num) {
      productOfDenAndDis(i) = localDensity(i) * distanceToHighDensity(i)
    }

    val productOfDenAndDisSortedRdd = sc.makeRDD(productOfDenAndDis).zipWithIndex().sortBy(-_._1).zipWithIndex()
/*
  println("排序后的乘积")
  for(i<- productOfDenAndDisSortedRdd){
    println(i)
  }
*/
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
        val x=distanceToHighDensityId(id_i)

        val label=clusterMap(x)
        clusterMap += (id_i -> label)
      }

    }

    println("聚类过程所用的时间是:" + (System.currentTimeMillis() - begin4))

    println("time for cluster is:" + (System.currentTimeMillis() - begin))

     for(i<- clusterMap){
       println(i._1+" "+i._2)
     }
/*
    println("聚类结果")

    for(i<- clusterMap) {
    println(i._1 + " " + i._2)
  }
*/
  }

  //DTW距离 scala版本
  def DTW(xs: Array[Double], ys: Array[Double]) = {
   // println("ok")
    val xLen = xs.length
    val yLen = ys.length
    //计算xs和ys的距离矩阵
    val DTWMatrix = Array.ofDim[Double](xLen, yLen)
    for (i <- 0 until xLen) {
      for (j <- 0 until yLen) {
        DTWMatrix(i)(j) = Math.pow((xs(i) - ys(j)), 2)
      }
    }
    //特殊计算第一行和第一列
    for (i <- 1 until yLen) {
      DTWMatrix(0)(i) = DTWMatrix(0)(i - 1) + DTWMatrix(0)(i)
    }
    for (i <- 1 until xLen) {
      DTWMatrix(i)(0) = DTWMatrix(i - 1)(0) + DTWMatrix(i)(0)
    }
    //计算接下来的行和列
    for (i <- 1 until xLen) {
      for (j <- 1 until yLen) {
        val d1 = DTWMatrix(i)(j - 1) + DTWMatrix(i)(j)
        val d2 = DTWMatrix(i - 1)(j) + DTWMatrix(i)(j)
        val d3 = DTWMatrix(i - 1)(j - 1) + DTWMatrix(i)(j)
        var minDist = Math.min(d1, d2)
        minDist = Math.min(minDist, d3)
        DTWMatrix(i)(j) = minDist
      }
    }
    Math.sqrt(DTWMatrix(xLen - 1)(yLen - 1))
  }
  //计算欧式距离矩阵
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
  //计算LB_keogh矩阵
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











