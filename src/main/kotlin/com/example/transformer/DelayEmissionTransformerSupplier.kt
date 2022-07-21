package com.example.transformer

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import java.time.Duration

class DelayEmissionTransformerSupplier<K, V, S>(
    private val storeName: String,
    private val timeout: Duration,
    private val punctuationInterval: Duration,
    private val keySerde: Serde<S>,
    private val valSerde: Serde<V>,
    private val keyConverter: (Windowed<K>) -> S
) : TransformerSupplier<Windowed<K>, V?, KeyValue<S, V>?> {
    override fun get(): Transformer<Windowed<K>, V?, KeyValue<S, V>?> {
        return DelayEmissionTransformer(
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
