package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class Config(path: String = "TPCH", workers: Int = 4)

object SimpleSpark extends App {

	override def main(args: Array[String]): Unit = {

		// Turn off logging
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		def time[R](block: => R): R = {
			val t0 = System.currentTimeMillis()
			val result = block
			val t1 = System.currentTimeMillis()
			println(s"Execution: ${t1 - t0} ms")
			result
		}

		val parser = new scopt.OptionParser[Config]("IND Finder") {
			head("IND Finder", "4.2")

			opt[String]('p', "path").action((x, c) =>
				c.copy(path = x)).text("path is the location of the table files")

			opt[Int]('c', "cores").action((x, c) =>
				c.copy(workers = x)).text("cores is the number of workers")
		}

		parser.parse(args, Config()) match {
			case Some(config) =>
				val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
					.map(name => config.path + s"/tpch_$name.csv")


				val sparkBuilder = SparkSession
					.builder()
					.appName("SparkTutorial")
					.master("local[" + config.workers + "]")
				val spark = sparkBuilder.getOrCreate()

				time {
					Sindy.discoverINDs(inputs, spark)
				}
			case None =>
				println("Nope")
		}
	}
}
