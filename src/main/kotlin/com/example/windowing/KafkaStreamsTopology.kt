package com.example.windowing

import com.example.PageId
import com.example.PageRating
import com.example.PageRatingAgg
import com.example.WindowedPageId
import com.example.transformer.DelayEmissionProcessorSupplier
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import java.lang.Integer.max
import java.lang.Integer.min
import java.time.Duration


@EnableKafkaStreams
@Configuration
class KafkaStreamsTopology() {

    private val windowedKeySerde: SpecificAvroSerde<WindowedPageId> = createSerde(true)
    private val valSerde: SpecificAvroSerde<PageRatingAgg> = createSerde(false)

    private val ratingAggregator = Aggregator<PageId, PageRating, PageRatingAgg> { pageId, rating, aggregate ->
        aggregate.count += 1
        aggregate.sum += rating.rating
        aggregate.avg = aggregate.sum / aggregate.count.toDouble()
        aggregate.max = max(aggregate.max, rating.rating)
        aggregate.min = min(aggregate.min, rating.rating)
        return@Aggregator aggregate
    }

    @Bean
    fun kStream(
        kStreamBuilder: StreamsBuilder
    ): Topology {
        val contractStream = kStreamBuilder.stream<PageId, PageRating>("ratings")
        val timeWindow = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(1))
        val storeName = "suppressionStore"
        val resultStream = contractStream.groupByKey()
            .windowedBy(timeWindow)
            .aggregate(
                Initializer { PageRatingAgg.newBuilder().build() },
                ratingAggregator
            )
            .toStream()
            .process(
                DelayEmissionProcessorSupplier(
                    storeName = storeName,
                    keySerde = windowedKeySerde,
                    valSerde = valSerde,
                    keyConverter = ::toWindowedKey,
                    timeout = Duration.ofSeconds(5),
                    punctuationInterval = Duration.ofMillis(500)
                )
            )

        resultStream.print(Printed.toSysOut())
        return kStreamBuilder.build()
    }

    private fun toWindowedKey(windowed: Windowed<PageId>): WindowedPageId {
        return WindowedPageId.newBuilder()
            .setId(windowed.key().id)
            .setWindowStart(windowed.window().start())
            .setWindowEnd(windowed.window().end())
            .build()
    }

    private final fun <T : SpecificRecord?> createSerde(isKey: Boolean): SpecificAvroSerde<T> {
        return SpecificAvroSerde<T>().also {
            it.configure(mutableMapOf(SCHEMA_REGISTRY_URL_CONFIG to "http://localhost:8081"), isKey)
        }
    }

}


