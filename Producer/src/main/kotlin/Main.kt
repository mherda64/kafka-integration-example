import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

private fun createProducer(broker: String): Producer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = broker
    props["key.serializer"] = StringSerializer::class.java.canonicalName
    props["value.serializer"] = StringSerializer::class.java.canonicalName
    return KafkaProducer<String, String>(props)
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

val inputTopic = "input-topic"
val broker = "localhost:9092"

fun main() {
    val producer = createProducer(broker)
    val runId = UUID.randomUUID().toString()
    println("Producer Instance $runId")
    for (i in 0..100_000) {
        sendInputData(
            producer,
            inputTopic,
            jsonMapper.writeValueAsString(
                InputData(i, runId, (1..10).random())
            )
        )
    }
    producer.flush()
}

fun sendInputData(producer: Producer<String, String>, topic: String, value: String) {
    producer.send(ProducerRecord(topic, value))
}