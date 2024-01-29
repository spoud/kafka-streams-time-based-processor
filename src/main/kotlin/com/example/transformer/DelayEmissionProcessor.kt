package com.example.transformer

import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import java.time.Duration
import java.time.Instant

class DelayEmissionProcessor<K, V, S>(
    private val storeName: String,
    private val suppressTimeout: Duration,
    private val punctuationInterval: Duration,
    private val keyConverter: (Windowed<K>) -> S,
) : Processor<Windowed<K>, V?, S, V?> {
    private lateinit var context: ProcessorContext<S, V?>
    private lateinit var stateStore: TimestampedKeyValueStore<S, V>

    override fun init(context: ProcessorContext<S, V?>) {
        this.stateStore = context.getStateStore(storeName)
        this.context = context
        context.schedule(punctuationInterval, PunctuationType.WALL_CLOCK_TIME, ::emitEventsWithoutRecentUpdates)
    }

    override fun close() {
        // nothing to do
    }

    override fun process(record: Record<Windowed<K>, V?>) {
        val wallclockTime = Instant.now().toEpochMilli()
        stateStore.put(keyConverter(record.key()), ValueAndTimestamp.make(record.value(), wallclockTime))
    }

    private fun emitEventsWithoutRecentUpdates(timestamp: Long) {
        stateStore.all().use { iterator ->
            while (iterator.hasNext()) {
                val keyValue = iterator.next()
                // check for last update (based on wallclock time that we set on line 34)
                if (timestamp - keyValue.value.timestamp() > suppressTimeout.toMillis()) {
                    // emit event and remove from state store
                    val record = Record<S, V>(keyValue.key, keyValue.value.value(), timestamp)
                    context.forward(record)
                    stateStore.delete(keyValue.key)
                }
            }
        }
    }
}
