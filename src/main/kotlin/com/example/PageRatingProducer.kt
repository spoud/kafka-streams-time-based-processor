package com.example

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import kotlin.random.Random


@Service
class PageRatingProducer(
    private val kafkaTemplate: KafkaTemplate<PageId, PageRating>,
) {

    @Scheduled(fixedRate = 1000)
    fun sendContractEvents() {
        val pageId = Random.nextLong(2)
        sendRandomRating(pageId)
        pause()
        sendRandomRating(pageId)

    }

    private fun pause() {
        Thread.sleep(Random.nextLong(20, 500))
    }

    private fun sendRandomRating(pageId: Long) {
        val rating = PageRating.newBuilder().setRating(Random.nextInt(1, 6)).build()
        kafkaTemplate.send("ratings", PageId(pageId), rating)
    }


}
