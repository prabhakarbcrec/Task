package com.invitas.task;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import con.invitas.taskAll.Interfaces.GetAllFrunctonHere;

public class GetJsonObject implements GetAllFrunctonHere, Serializable {
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static Properties props=null;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void MakeJsonObjectAndReturnItAsString(String DeviceId, String DataTime, String CurrentA, String CurrentB)
			throws ParseException {
		JsonObject finalJson = new JsonObject();
		finalJson.addProperty("deviceId", Integer.valueOf(DeviceId));
		finalJson.addProperty("dataTime", sdf.parse(DataTime).getTime());
		JsonObject jsonA = new JsonObject();
		jsonA.addProperty("dataName", "CurrentA");
		jsonA.addProperty("dataValue", Float.valueOf(CurrentA));
		JsonObject jsonB = new JsonObject();
		jsonB.addProperty("dataName", "CurrentB");
		jsonB.addProperty("dataValue", Float.valueOf(CurrentB));
		JsonArray array = new JsonArray();
		array.add(jsonA);
		array.add(jsonB);

		finalJson.add("dataList", array);
		ImportJsonDataIntoKafka(finalJson.toString());
		

	}

	@Override
	public void ImportJsonDataIntoKafka(String finalData) {
		if (props==null) {
			props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		}
		Producer<String, String> producer = new KafkaProducer<>(props);
		producer.send(new ProducerRecord<String, String>("sample-data", finalData, finalData));
		producer.close();
		System.out.println("message:  " + finalData + "  has sent to Topic:  sample-data");

	}

}
