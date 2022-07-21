package com.example

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling


@SpringBootApplication
@EnableScheduling
class PageRatingApplication

fun main(args: Array<String>) {
    runApplication<PageRatingApplication>(*args)
}
