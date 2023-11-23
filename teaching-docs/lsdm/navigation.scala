package navigation

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Navigation {

        // Finds out the index of "name" in the array firstLine 
        // returns -1 if it cannot find it
	def findCol(firstLine:Array[String], name:String):Int = {
		if (firstLine.contains(name)) { firstLine.indexOf(name) }
		else { -1}
	}


	def main(args: Array[String]): Unit = {
		// start spark with 1 worker thread
		val conf = new SparkConf().setAppName("CLIWOC").setMaster("local[1]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		// read the input file into an RDD[String]
		val wholeFile = sc.textFile("../data/CLIWOC15.csv")
                
		// The first line of the file defines the name of each column in the cvs file
                // We store it as an array in the driver program
		val firstLine = wholeFile.filter(_.contains("RecID")).collect()(0).filterNot(_ == '"').split(',')
                
		// filter out the first line from the initial RDD
		val entries_tmp = wholeFile.filter(!_.contains("RecID"))

                // split each line into an array of items
                val entries = entries_tmp.map(x => x.split(','))

                // keep the RDD in memory
                entries.cache()

                // ##### Create an RDD that contains all nationalities observed in the
                // ##### different entries

                // Information about the nationality is provided in the column named "Nationality"

                // First find the index of the column corresponding to the "Nationality"
                val column_index=findCol(firstLine, "Nationality")
                println("Nationality corresponds to column "+ column_index.toString)

                // Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates 
                val nationalities = entries.map(x => x(column_index)).distinct()

                // Display the 5 first nationalities
                println("A few examples of nationalities:")
                nationalities.sortBy(x => x).take(5).foreach(println)

          // prevent the program from terminating immediatly
          println("Press Enter to continue...")
          scala.io.StdIn.readLine()
	}
}

