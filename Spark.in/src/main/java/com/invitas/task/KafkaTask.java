package com.invitas.task;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 
 * @author Prabhakar Kumar Ojha
 *
 */

public class KafkaTask implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	GetJsonObject getjsonobject = null;;
	SparkSession spark;

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		new KafkaTask().ReadCSV();

	}

	public void ReadCSV() {
		getjsonobject = new GetJsonObject();
		spark = SparkSession.builder().appName("invitasTastToComplete").master("local[*]").getOrCreate();
		Dataset<Row> AllCsvData = spark.read().format("com.databricks.spark.csv").option("header", true)
				.option("delimiter", ",")
				.load("file:///home/hadoop/Documents/workspace-sts-3.9.11.RELEASE/Spark.in/src/main/java");
		AllCsvData.coalesce(1).foreach(DataFromCsv -> {
			getjsonobject.MakeJsonObjectAndReturnItAsString(DataFromCsv.getAs("DeviceID"),
					DataFromCsv.getAs("DataTime"), DataFromCsv.getAs("CurrentA"), DataFromCsv.getAs("CurrentB"));
		});

	}

}
