# Example Kafka Streams transformation

When we use time-based operations in a Kafka Streams application, then time is usually extracted from records in a stream.
This event-time differs from processing-time (wallclock-time).

In this sample application we use a custom transformation to suppress event emission of windowed operations based on wallclock time,
while still working with event-time in our windowing operation.


## Sample Use Case

Our Spring Boot App has a producer bean which creates random page ratings (1-5) in a fixed interval.
We aggregate these ratings per PageId in time windows.
A custom tranformation is used to suppress updates as long as new ratings arrive for a time window.  

## Running the app

- Start docker compose with `docker-compose up -d`
- Test build with `./mvnw clean package`
- Then start the app using the `main` function in `PageRatingAplication.kt`
