package spark.example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._

/**
 * This example illustrates how you can use Apache Spark to process JSON messages
 * stored in a file, in this case we acquired the data from Datasift (www.datasift.com)
 */
object CandyCrushExample {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "CandyCrushExample")
    

    // We want to extract the level number from "Yay, I completed level 576 in Candy Crush Saga!"
    // the actual text will change based on the users language but parsing the 'last number' works
    val pattern = """(\d+)""".r

    
    // Produces a RDD[String] containing all lines in the file
    val lines = sc.textFile("data/facebook-2014-05-19.json")
    
    
    // Process the messages
    val results = lines.map(line => {
      // Parse the JSON
      parse(line)
    }).filter(json => {
      // Filter out only 'Candy Crush Saga' activity
      get(json \ "facebook" \ "application").toString == "Candy Crush Saga"
    }).map(json => {
      // Extract the 'level' or default to zero
      var level = 0;
      pattern.findAllIn( get(json \ "interaction" \ "title").toString ).matchData.foreach(m => {
        level = m.group(1).toInt
      })
      // Extract the gender
      val gender = get(json \ "demographic" \ "gender").toString
      // Return a Tuple of RDD[gender: String, (level: Int, count: Int)] 
      ( gender, (level, 1) )
    }).filter(a => {
      // Filter out entries with a level of zero
      a._2._1 > 0
    }).reduceByKey( (a, b) => {
      // Sum the levels and counts so we can average later
      ( a._1 + b._1, a._2 + b._2 )
    })
    
    
    // Print the results
    println("Game level by Gender:\n=====================")
    results.collect().foreach(entry => {
      val gender = entry._1
      val values = entry._2
      val average = values._1 / values._2
      println(gender + ": average=" + average + ", count=" + values._2 )
    })
    
    
    /* Example output shows that 'female' users have a high average reported level compared to 'male' users
       "female":        average=114, count=15422
       "male":          average=104, count=14727
       "mostly_male":   average=97, count=2824
       "mostly_female": average=99, count=1934
       "unisex":        average=113, count=2674
     */
    
    sc.stop()
  }

  // Utility method to unpack the value from JSON
  def get(value: JValue) = {
    value match {
      case JNothing | JNull => ""
      case JString(s)    => s
      case JDouble(num)  => num
      case JDecimal(num) => num
      case JInt(num)     => num
      case JBool(value)  => value
      case _ => ""
    }
  }
  
}