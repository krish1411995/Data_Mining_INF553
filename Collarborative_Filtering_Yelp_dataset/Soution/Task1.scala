import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

object Task1{


  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {
    val time1 = System.currentTimeMillis()
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    // Load and parse the data
    val data = sc.textFile(args(0))//"/Users/krishmehta/Desktop/DataMining/Krish_Mehta_HW2/hw2/Data/train_review.csv"
      .mapPartitionsWithIndex({
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      })
      .map(_.split(','))

    // Hashmap to store unique number for the user and business as it is in String
    val train_rating=data.map(l=>Rating(l(0).hashCode,l(1).hashCode,l(2).toDouble))


    // Build the recommendation model using ALS
    val rank = 2
    val numIterations = 20
    val model = ALS.train(train_rating, rank, numIterations, 0.25,1,1)

    // Evaluate the model on rating data////////////////////////

    // fetching the data again from the test data
    val test_data = sc.textFile(args(1))//"/Users/krishmehta/Desktop/DataMining/Krish_Mehta_HW2/hw2/Data/test_review.csv"
      .mapPartitionsWithIndex({
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      })
      .map(_.split(','))
    val userBusiness=test_data.map(l=>Rating(l(0).hashCode,l(1).hashCode,l(2).toDouble))
    val usersProducts = userBusiness.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val user_rdd = test_data.map(l=>l(0))
    val user_test=user_rdd.collect().toList
    val user_rdd_hashcode = test_data.map(l=>l(0).hashCode)
    val user_hashcode=user_rdd_hashcode.collect().toList

    val business_rdd =  test_data.map(l=>l(1))
    val business_test=business_rdd.collect().toList
    val business_rdd_hashcode =  test_data.map(l=>l(1).hashCode)
    val business_hashcode=business_rdd_hashcode.collect().toList


    // predicting the number
    val predictions = model.predict(usersProducts).map{ case Rating(x, y, z) => if (z<0) ((x, y), 0.00000) else if (z>5) ((x, y), 5.00000) else ((x, y), z)}

    // joining the predictions and the original test data to compare
    val ratesAnd = userBusiness.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    // comparing the original result with the predicted results
    val MSE = ratesAnd.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    val  RMSE=math.sqrt(MSE)
    //RMSE = (math floor RMSE * 100) / 100



    val diff= ratesAnd.map { case ((user, product), (r1, r2)) => math.abs(r1 - r2)}

    var num1=0
    var num2=0
    var num3=0
    var num4=0
    var num5=0

    for (item <- diff.collect) {
      item match
      {
        case item if (item>=0 && item<1) => num1 = num1 + 1;
        case item if (item>=1 && item<2) => num2 = num2 + 1;
        case item if (item>=2 && item<3) => num3 = num3 + 1;
        case item if (item>=3 && item<4) => num4 = num4 + 1;
        case item if (item>=4 ) => num5 = num5 + 1;
      }
    }

    println(">=0 and <1:"+ num1)
    println(">=1 and <2:"+ num2)
    println(">=2 and <3:"+ num3)
    println(">=3 and <4:"+ num4)
    println(">=4 :"+ num5)
    println("RMSE = " + RMSE)
    val time2 = System.currentTimeMillis()
    println((time2-time1)/1000)
    val output = predictions.map{ case((x, y), z) => (user_test(user_hashcode.indexOf(x)),business_test(business_hashcode.indexOf(y)), z)}
    val openFile = new PrintWriter(new File("Krish_Mehta_ModelBasedCF.txt"))
    openFile.write("userID,movieID,rating\n")
    val output1 = output.sortBy(r => (r._1, r._2, r._3)).collect.toList
    for(line <- output1){
      openFile.write(line._1+","+line._2+","+line._3+"\n")
    }
    openFile.close()


  }

}

