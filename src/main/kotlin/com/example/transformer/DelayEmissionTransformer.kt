package com.example.transformer

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import java.time.Duration
import java.time.Instant

class DelayEmissionTransformer<K, V, S>(
    private val storeName: String,
    private val suppressTimeout: Duration,
    private val punctuationInterval: Duration,
    private val keyConverter: (Windowed<K>) -> S,
) : Transformer<Windowed<K>, V?, KeyValue<S, V>?> {
    private lateinit var context: ProcessorContext
    private lateinit var stateStore: TimestampedKeyValueStore<S, V>

    override fun init(context: ProcessorContext) {
        stateStore = context.getStateStore(storeName)
        this.context = context
        context.schedule(punctuationInterval, PunctuationType.WALL_CLOCK_TIME, ::emitEventsWithoutRecentUpdates)
    }

    override fun close() {
        // nothing to do?
    }

    override fun transform(window: Windowed<K>, value: V?): KeyValue<S, V>? {
        val wallclockTime = Instant.now().toEpochMilli()
        stateStore.put(keyConverter(window), ValueAndTimestamp.make(value, wallclockTime))
        return null
    }

    private fun emitEventsWithoutRecentUpdates(timestamp: Long) {
        stateStore.all().use { iterator ->
            while (iterator.hasNext()) {
                val keyValue = iterator.next()
                if (timestamp - keyValue.value.timestamp() > suppressTimeout.toMillis()) {
                    context.forward(keyValue.key, keyValue.value.value())
                    stateStore.delete(keyValue.key)
                }
            }
        }
    }
}
