## This is my implementation of the project found on this tutorial from Confluent cloud

`
https://www.youtube.com/watch?v=jItIQ-UvFI4&t=1937s&ab_channel=Confluent
`

This project is a real-time YouTube video data tracker. It uses the YouTube Data API to fetch data about videos in a specific playlist and publishes this data to a Kafka topic. The Python script youtube_watcher.py is responsible for fetching the video data and producing it to Kafka. It fetches data such as the video title, view count, like count, and comment count. The script runs continuously, fetching new data every 10 seconds to ensure it captures any changes in real-time.

The queries.sql file contains KSQL queries that are used to process the data in the Kafka topic. These queries create streams and tables from the Kafka topic and perform transformations on the data. For example, one query creates a stream that filters out any records where the number of likes has not changed. This allows the system to focus on videos where the like count is actively changing.


A deeper explanation can be found at:

https://medium.com/@ofelipefernandez/building-a-youtube-data-analysis-telegram-bot-powered-by-kafka-and-ksqldb-9b6ee430f2c8

