# Example for a custom Kafka Streams processor with punctuations

This is the demo code for the article [Kafka Streams - Working with Time (medium.com)](https://spoud-io.medium.com/kafka-streams-working-with-time-d8bd3f1960e2)


When we use time-based operations in a Kafka Streams application, then time is usually extracted from records in a stream.
This event-time differs from processing-time (wallclock-time).

In this sample application we use a custom transformation to suppress event emission of windowed operations based on wallclock time,
while still working with event-time in our windowing operation.


## Sample Use Case

Our Spring Boot App has a producer bean which creates random page ratings (1-5) in a fixed interval.
We aggregate these ratings per PageId in time windows.
A custom processor is used to suppress updates as long as new ratings arrive for a time window.  

## Running the app

- Start docker compose with `docker-compose up -d`
- Test build with `./mvnw clean package`
- Then start the app using the `main` function in `PageRatingAplication.kt`
