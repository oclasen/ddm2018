package de.hpi.spark_tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object Sindy {

	def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

		import spark.implicits._


		var tablesBuffer = new ListBuffer[DataFrame]()

		var rddBuffer = new ListBuffer[RDD[(String, Set[String])]]
		for (i <- inputs.indices) {
			tablesBuffer += spark.read
				.option("inferSchema", "true")
				.option("header", "true")
				.option("sep", ";")
				.csv(inputs(i))
		}

		val tableList = tablesBuffer.toList

		tableList.foreach(df => df.columns.foreach(col =>
			rddBuffer += df.select(col).dropDuplicates().rdd.map(r => (r(0).toString, Set[String](col)))
		))


		//create one rdd containing the tuples
		val rdd = rddBuffer.toList.reduce(_ union _)

		//create attributeSets
		val attributeSets = rdd.reduceByKey(_ ++ _).values.distinct()

		//create inclusionLists
		val inclusionLists = attributeSets.flatMap(set => set.map(attribute => (attribute, set - attribute)))

		//intersect inclusionLists and remove empty intersects
		val aggregateSets = inclusionLists.reduceByKey(_.intersect(_)).filter(_._2.nonEmpty).sortByKey(numPartitions = 1)

		//print the INDs
		aggregateSets.foreach(row => {
			println(row._1 + " < " + row._2.mkString(", "))
		})
	}
}
