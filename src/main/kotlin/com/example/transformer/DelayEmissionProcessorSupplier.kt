package com.example.transformer

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import java.time.Duration

class DelayEmissionProcessorSupplier<K, V, S>(
    private val storeName: String,
    private val timeout: Duration,
    private val punctuationInterval: Duration,
    private val keySerde: Serde<S>,
    private val valSerde: Serde<V>,
    private val keyConverter: (Windowed<K>) -> S
) : ProcessorSupplier<Windowed<K>, V?, S, V?> {


    override fun get(): Processor<Windowed<K>, V?, S, V?> {
        return DelayEmissionProcessor(
            storeName = storeName,
            suppressTimeout = timeout,
            punctuationInterval = punctuationInterval,
            keyConverter = keyConverter
        )
    }

    override fun stores(): Set<StoreBuilder<*>> {
        val store = Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore(storeName),
            keySerde,
            valSerde
        )
        return setOf(store)
    }
}
