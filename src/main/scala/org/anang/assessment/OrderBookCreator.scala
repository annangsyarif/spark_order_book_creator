package org.anang.assessment

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.logging.Logger
import org.apache.kafka.clients.producer.KafkaProducer

object OrderBookCreator {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  // read config file
  lazy val config_file = ConfigFactory.load()

  // create kafka producer
  lazy val kafka_producer: KafkaProducer[String, String ] = kafkaFunction
    .createProducer(
      config_file.getString("kafka.host"),
      config_file.getInt("kafka.port")
    )

  def main(args: Array[String]): Unit = {

    // initiate spark
    val spark = SparkSession.builder
      .appName(config_file.getString("AppName"))
      .master(config_file.getString("spark.master-url"))
      .config("spark.ui.reverseProxy", "true")
      .config("spark.ui.reverseProxyUrl", "http://localhost:8080")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

    // initiate streaming context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    // generate kafka config
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> s"${config_file.getString("kafka.host")}:${config_file.getString("kafka.port")}",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "order_book_creator",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val kafkaSourceTopics = Array(config_file.getString("kafka.order-topic"))

    // create kafka direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](kafkaSourceTopics, kafkaParams)
    )

    // extract value from kafka
    val valuesStream: DStream[String] = kafkaStream.map(record => record.value())

    // create empty accumulated data
    var accumulatedData: Seq[String] = Seq.empty[String]

    // add data to accumulated data
    valuesStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        accumulatedData = accumulatedData ++ rdd.collect()

        if (accumulatedData.nonEmpty) {
          // parsing json and change to dataframe
          val jsonRDD = spark.sparkContext.parallelize(accumulatedData)
          val raw_df = spark.read.json(jsonRDD)

          // transformation
          val summaryDF = DataTransformation.processData(raw_df)
          // summaryDF.show()
         val jsonArrayString = DataTransformation.dfToJson(summaryDF)

         // send to kafka
         kafkaFunction.sendToKafka(
           kafka_producer,
           config_file.getString("kafka.order-book-topic"),
           jsonArrayString
         )
        }
      }
    }

    // handle shutdown hook
    sys.addShutdownHook {
      logger.info("Stopping ...")
      
      kafka_producer.close()
    }

    // start the streaming
    logger.info("Listening from Kafka")
    ssc.start()
    ssc.awaitTermination()
  }
}
