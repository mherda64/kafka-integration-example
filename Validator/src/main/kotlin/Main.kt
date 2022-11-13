import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap

private fun createConsumer(broker: String, consumerGroup: String): Consumer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = broker
    props["group.id"] = consumerGroup
    props["max.poll.records"] = 1000
    props["key.deserializer"] = StringDeserializer::class.java
    props["value.deserializer"] = StringDeserializer::class.java
    return KafkaConsumer<String, String>(props)
}

val jsonMapper = ObjectMapper().apply {
    registerKotlinModule()
    disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    setDateFormat(StdDateFormat())
}

data class OutputData(
    val index: Int,
    val runId: String,
    val timeToProcess: Int,
    val processedBy: String
)

data class StatisticData(
    val runId: String,
    val numberOfRecords: Int,
    val avgProcessingTime: Double,
    val processedByMap: Map<String, Int>


)

val outputTopic = "output-topic"
val broker = "localhost:9092"
val validatorConsumerGroup = "validator-group"

fun main() {
    val consumer = createConsumer(broker, validatorConsumerGroup)
    var consumedData = ConcurrentHashMap<String, MutableList<OutputData>>()

    val instance = UUID.randomUUID().toString()
    println("Validator Instance: $instance")

    consumer.subscribe(listOf(outputTopic))

    var i = 0;

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        i++;
        records.iterator().forEach {
            val processedData = jsonMapper.readValue(it.value(), OutputData::class.java)

            if (!consumedData.containsKey(processedData.runId))
                consumedData[processedData.runId] = mutableListOf()

            consumedData[processedData.runId]!!.add(processedData)

        }

        if (i > 100) {
            i = 0
            val map = consumedData.map {
                StatisticData(
                    runId = it.key,
                    numberOfRecords = it.value.size,
                    avgProcessingTime = it.value.map { it.timeToProcess }.average(),
                    processedByMap = it.value.groupBy { it.processedBy }.mapValues { it.value.size }
                )
            }

            println(jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(map))
        }
    }
}