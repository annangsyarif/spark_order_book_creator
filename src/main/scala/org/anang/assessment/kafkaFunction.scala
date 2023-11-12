package org.anang.assessment

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

object kafkaFunction {

  def createProducer(
                  host: String,
                  port: Int): 
  KafkaProducer[String, String] = {
      // define kafka properties
      val kafkaProps = new java.util.Properties()
      kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"$host:$port")
      kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

      // create producer
      new KafkaProducer[String, String](kafkaProps)
  }

  def sendToKafka(
                 producer: KafkaProducer[String, String],
                 topic: String,
                 message: String):
  Unit = {
    // add the message
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
  }
}
