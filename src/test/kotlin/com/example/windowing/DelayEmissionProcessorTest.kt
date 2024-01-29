package com.example.windowing

import com.example.transformer.DelayEmissionProcessorSupplier
import org.apache.kafka.common.metrics.Sensor.RecordingLevel
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.ForeachAction
import org.apache.kafka.streams.kstream.SessionWindows
import org.apache.kafka.test.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

internal class DelayEmissionProcessorTest {
    private val topic = "stream"
    private val stateStoreName: String = "myTransformState"
    private val results: MutableList<KeyValue<Int, Long>> = mutableListOf()
    private val accumulateExpected = ForeachAction { key: Int, value: Long -> results.add(KeyValue.pair(key, value)) }
    private lateinit var builder: StreamsBuilder

    @BeforeEach
    fun before() {
        builder = StreamsBuilder().also { builder ->
            // given streams topology
            builder.stream(topic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(10)))
                .count()
                .toStream()
                .process(
                    DelayEmissionProcessorSupplier<Int, Long, Int>(
                        keySerde = Serdes.Integer(),
                        valSerde = Serdes.Long(),
                        storeName = stateStoreName,
                        timeout = Duration.ofMillis(500),
                        punctuationInterval = Duration.ofMillis(50),
                        keyConverter = { windowed -> windowed.key() }
                    )
                )
                .foreach(accumulateExpected)
        }
    }

    @Test
    fun `window should buffer events and emit when no updates were received for given time`() {
        // when data is ingested
        val props: Properties = getStreamsConfig(Serdes.Integer(), Serdes.Integer())
        TopologyTestDriver(builder.build(), props).use { driver ->
            val inputTopic = driver.createInputTopic(topic, IntegerSerializer(), IntegerSerializer())

            // 3 events cause a single output event after a delay
            inputTopic.pipeKeyValueList(listOf(KeyValue(1, 1), KeyValue(2, 1), KeyValue(1, 1)))
            driver.advanceWallClockTime(Duration.ofMillis(2000))
            assertThat(results).isEqualTo(listOf(KeyValue.pair(1, 2L), KeyValue.pair(2, 1L)))

            results.clear()

            // 3 more events go to the same time window and increase the counts
            inputTopic.pipeKeyValueList(listOf(KeyValue(1, 1), KeyValue(2, 1), KeyValue(1, 1)))
            assertThat(results).isEmpty()
            driver.advanceWallClockTime(Duration.ofMillis(2000))
            assertThat(results).isEqualTo(listOf(KeyValue.pair(1, 4L), KeyValue.pair(2, 2L)))
        }
    }

    private fun getStreamsConfig(
        keyDeserializer: Serde<*>,
        valueDeserializer: Serde<*>
    ): Properties {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = UUID.randomUUID().toString()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9091"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = keyDeserializer.javaClass.name
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = valueDeserializer.javaClass.name
        props[StreamsConfig.STATE_DIR_CONFIG] = TestUtils.tempDirectory().path
        props[StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG] = RecordingLevel.DEBUG.name
        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 100
        return props
    }


}
