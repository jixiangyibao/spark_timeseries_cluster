
import java.awt.Dimension
import scala.collection.mutable.Map
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter}
import scala.swing.event.ButtonClicked
import scala.swing.{Button, FileChooser, FlowPanel, Label, MainFrame, SimpleSwingApplication, TextField}
/**
  * Created by Administrator on 2016/8/10.
  */
object timeSeriesCluster extends SimpleSwingApplication {
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
  val inf=999999999.0
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
    val dataRdd = sc.textFile(filePath).map(x=>x.split(",").map(x=>x.toDouble))
    val num=dataRdd.count().toInt//耗时
    val data=dataRdd.collect()//耗时
    //手动调节参数1：阈值dc
    val dc=2//手动调节参数2：聚类数k
    val k=18
    val begin = System.currentTimeMillis()
    //计算全对距离矩阵DisMat
    val DisMat=calDisMat(data)
    val dataRddIndexed=dataRdd.zipWithIndex()
    //计算局部密度localDen
    val localDen=dataRddIndexed.map{
      case(rowData,index)=>{
        var count=0
        for(i<-0 until num){
          if(i!=index){
            if(DisMat(index.toInt)(i)<dc)
              count+=1
          }
        }
        (index.toInt,count)
      }
    }
    val localDensity=localDen.map{
      case(id,count)=>
        count
    }.collect()
    //对局部密度进行降序排序
    val localDenSorted=localDen.sortBy(-_._2).zipWithIndex().map{
      case((ind1,count),ind2)=>{
        (ind2,ind1,count)
      }
    }
   val localDenSortedColl=localDenSorted.collect()
    val maxLocalDensityId=localDenSortedColl(0)._2
    //输出数据编号及其局部密度值
   println("局部密度")
   for(i<-localDenSorted)
     println(i)
  //计算更高局部密度值的点集合
  // val maxDisToHighIndex=localDenSorted.take(1)(0)._1
   val disToHighSorInd=localDenSorted.map{
    case(ind1,ind2,count)=>
      ind2
  }.collect()
   val disToHigh=localDenSorted.map {
     case (index1,index2,count) => {
       var minimum = -1.0
       var mininumInd=0
       if (index1 != 0) {
         for (i <- 0 until index1.toInt) {
           val dis = DisMat(index2)(disToHighSorInd(i))
           if (minimum == -1 || minimum < dis){
             minimum = dis
             mininumInd=disToHighSorInd(i)
           }
         }
       }
       (index1,index2,mininumInd,minimum)
     }
   }
    val DisToHigh=disToHigh.map{
      case(ind1,ind2,disInd,dis)=>{
        if(dis == -1.0)
          (ind1,ind2,disInd,inf)
        else
          (ind1,ind2,disInd,dis)
      }
    }
    val distanceToHighDensityId = Array.ofDim[Int](num)
    val DisToHighColl=DisToHigh.collect()
    for(i<-DisToHighColl){
     distanceToHighDensityId(i._2)=i._3
   }

    var disToHighMap: Map[Int, Int] = scala.collection.mutable.Map()

    for(i<-DisToHighColl){
      val key=i._2
      val value=i._3
      disToHighMap += (key->value)
    }
    //计算密度与距离的乘积
    val productOfDenAndDis = Array.ofDim[Double](num)
    for (i <- 0 until num) {
      productOfDenAndDis(i) = localDensity(i) * distanceToHighDensityId(i)
    }
    productOfDenAndDis(maxLocalDensityId)=inf
    val productOfDenAndDisSortedRdd = sc.makeRDD(productOfDenAndDis).zipWithIndex().sortBy(-_._1).zipWithIndex()

   val prod=productOfDenAndDisSortedRdd.collect()
    //选择前K个最大值作为聚类中心
  val cluster=productOfDenAndDisSortedRdd.map{
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
      val id_i=localDenSortedColl(i)._2.toInt
      if(!clusterMap.contains(id_i)){
        val label=clusterMap(distanceToHighDensityId(id_i))
        clusterMap += (id_i -> label)
      }

    }

    println("聚类所用的时间是:" + (System.currentTimeMillis() - begin))
    println("聚类结果：")
    for(i<-clusterMap){
      println(i._1+" "+i._2)
    }
    val writer = new PrintWriter(new File("C:\\Users\\刘咏\\IdeaProjects\\wordCount\\cluRes.txt" ))
    for(i<-clusterMap){
      writer.print(i._2)
      writer.print(" ")
    }
    writer.println("oooook")

  }
  //计算所有距离模块
  def calDisMat(data:Array[Array[Double]]): Array[Array[Double]] ={
    var cnt=0
    val numOfData=data.length
    val DisMat=Array.ofDim[Double](numOfData,numOfData)
    for(i<-0 until numOfData){
      for(j<-0 until numOfData){
       if(i==j){
          DisMat(i)(j)=0
        }
       else if(DisMat(j)(i)!=0){
         DisMat(i)(j)=DisMat(j)(i)
       }
        else {
          val distance=DTW(data(i),data(j))
         cnt+=1
          DisMat(i)(j)=distance
          DisMat(j)(i)=distance
        }
      }
    }
    println("DTW计算的次数是："+cnt)
    val writer = new PrintWriter(new File("C:\\Users\\刘咏\\IdeaProjects\\wordCount\\disMat.txt" ))
    writer.println("DTW距离矩阵")
    for(i<-0 until numOfData){
      for(j<-0 until numOfData){
        writer.print(DisMat(i)(j))
        writer.print(" ")
      }
      writer.println()
    }
    writer.close()

    return DisMat
  }
  //欧式距离
  def euclideanDistance(xs: Array[Double], ys: Array[Double]) = {
    Math.sqrt((xs zip ys).map {
      case (x, y) => Math.pow(y - x, 2)
    }.sum)
  }
  //DTW距离 scala版本
  def DTW(xs:Array[Double],ys:Array[Double])={
    val xLen=xs.length
    val yLen=ys.length
    //计算xs和ys的距离矩阵
    val DTWMatrix=Array.ofDim[Double](xLen,yLen)
    for(i<-0 until xLen){
      for(j<-0 until yLen){
        DTWMatrix(i)(j)=Math.pow((xs(i)-ys(j)),2)
      }
    }
    //特殊计算第一行和第一列
    for(i<-1 until yLen){
      DTWMatrix(0)(i)=DTWMatrix(0)(i-1)+DTWMatrix(0)(i)
    }
    for(i<-1 until xLen){
      DTWMatrix(i)(0)=DTWMatrix(i-1)(0)+DTWMatrix(i)(0)
    }
    //计算接下来的行和列
    for(i<-1 until xLen){
      for(j<-1 until yLen){
        val d1=DTWMatrix(i)(j-1)+DTWMatrix(i)(j)
        val d2=DTWMatrix(i-1)(j)+DTWMatrix(i)(j)
        val d3=DTWMatrix(i-1)(j-1)+DTWMatrix(i)(j)
        var minDist=Math.min(d1,d2)
        minDist=Math.min(minDist,d3)
        DTWMatrix(i)(j)=minDist
      }
    }
    Math.sqrt(DTWMatrix(xLen-1)(yLen-1))
  }
}