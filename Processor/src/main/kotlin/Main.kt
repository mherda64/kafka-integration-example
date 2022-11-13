import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.*

private fun createProducer(broker: String): Producer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = broker
    props["key.serializer"] = StringSerializer::class.java.canonicalName
    props["value.serializer"] = StringSerializer::class.java.canonicalName
    return KafkaProducer<String, String>(props)
}

private fun createConsumer(broker: String, consumerGroup: String): Consumer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = broker
    props["group.id"] = consumerGroup
    props["max.poll.records"] = 5
    props["key.deserializer"] = StringDeserializer::class.java
    props["value.deserializer"] = StringDeserializer::class.java
    return KafkaConsumer<String, String>(props)
}

val jsonMapper = ObjectMapper().apply {
    registerKotlinModule()
    disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    setDateFormat(StdDateFormat())
}

data class InputData(
    val index: Int,
    val runId: String,
    val timeToProcess: Int
)

data class OutputData(
    val index: Int,
    val runId: String,
    val timeToProcess: Int,
    val processedBy: String
)

val inputTopic = "input-topic"
val outputTopic = "output-topic"
val broker = "localhost:9092"
val processorConsumerGroup = "processor-group"

fun main() {
    val producer = createProducer(broker)
    val consumer = createConsumer(broker, processorConsumerGroup)

    val instance = UUID.randomUUID().toString()
    println("Processor Instance: $instance")

    consumer.subscribe(listOf(inputTopic))

    while (true) {
        println("Polling records")
        val records = consumer.poll(Duration.ofSeconds(1))
        println("Polled ${records.count()} records")
        records.iterator().forEach {
            val inputData = jsonMapper.readValue(it.value(), InputData::class.java)
            println("Read input data index [${inputData.index}], processing [${inputData.timeToProcess}] seconds")
            Thread.sleep((inputData.timeToProcess).toLong())
            println("Processed input data index [${inputData.index}], sending to output topic")
            producer.send(ProducerRecord(
                outputTopic,
                jsonMapper.writeValueAsString(OutputData(inputData.index, inputData.runId, inputData.timeToProcess, instance))
            ))
        }
    }
}