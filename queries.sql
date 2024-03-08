
#Creating the stream to read the data from the topic
CREATE STREAM youtube_videos_felipe (
  video_id VARCHAR KEY,
  title VARCHAR,
  views INTEGER,
  comments INTEGER,
  likes INTEGER
) WITH (
  KAFKA_TOPIC = 'youtube_videos_felipe',
  PARTITIONS = 1,
  VALUE_FORMAT = 'json'
);

#testing that the stream is reading the data from the topic
SELECT *
FROM  YOUTUBE_VIDEOS_FELIPE
EMIT CHANGES;

#Creating table to store the changes in the youtube videos and grouping by video_id
CREATE TABLE youtube_changes WITH (KAFKA_TOPIC='youtube_changes_felipe') AS SELECT
  video_id,
  latest_by_offset(title) AS title,
  latest_by_offset(comments, 2)[1] AS comments_previous,
  latest_by_offset(comments, 2)[2] AS comments_current,
  latest_by_offset(views, 2)[1] AS views_previous,
  latest_by_offset(views, 2)[2] AS views_current,
  latest_by_offset(likes, 2)[1] AS likes_previous,
  latest_by_offset(likes, 2)[2] AS likes_current
FROM youtube_videos
GROUP BY video_id
EMIT CHANGES;


#Testing that table is reading the changes in youtube
SELECT *
FROM  YOUTUBE_CHANGES
WHERE likes_current <> likes_previous
EMIT CHANGES;


#creating the stream to send the data to the telegram outbox
CREATE STREAM telegram_outbox_felipe (
  `chat_id` VARCHAR,
  `text` VARCHAR
) WITH (
  KAFKA_TOPIC='telegram_outbox_felipe', 
  PARTITIONS=1,
  VALUE_FORMAT='json'
);

#Testing for connection
INSERT INTO telegram_outbox_felipe (
  `chat_id`,
  `text`
) VALUES (
  '<your chat id>',
  'First test: Hello Felipe'
);

#Create stream to fill the gap between the table and telegram_outbox

CREATE STREAM youtube_changes_stream_felipe (
    `VIDEO_ID` VARCHAR,
    `TITLE` VARCHAR,
    `COMMENTS_PREVIOUS` INTEGER,
    `COMMENTS_CURRENT` INTEGER,
    `VIEWS_PREVIOUS` INTEGER,
    `VIEWS_CURRENT` INTEGER,
    `LIKES_PREVIOUS` INTEGER,
    `LIKES_CURRENT` INTEGER
  ) WITH (
    KAFKA_TOPIC='youtube_changes_felipe', 
    VALUE_FORMAT='json');

INSERT INTO telegram_outbox_felipe
SELECT
  '<your chat id>' AS `chat_id`,
  CONCAT(
    'Likes changed: ',
    CAST(likes_previous AS STRING),
    ' => ',
    CAST(likes_current AS STRING),
    '. ',
    title
  ) AS `text`
FROM youtube_changes_stream_felipe
WHERE likes_current <> likes_previous;

INSERT INTO telegram_outbox_felipe
SELECT
  '<your chat id>' AS `chat_id`,
  CONCAT(
    'Comments changed: ',
    CAST(comments_previous AS STRING),
    ' => ',
    CAST(comments_current AS STRING),
    '. ',
    title
  ) AS `text`
FROM youtube_changes_stream_felipe
WHERE comments_current <> likes_previous;

-- A views notification would be too chatty. Only send an alert if we
--  pass through a multiple of 200 views.
INSERT INTO telegram_outbox_felipe
SELECT
  '<your chat id>' AS `chat_id`,
  CONCAT(
    'Views changed: ',
    CAST(views_previous AS STRING),
    ' => ',
    CAST(views_current AS STRING),
    '. ',
    title
  ) AS `text`
FROM youtube_changes_stream_felipe
WHERE
  round(views_current / 200) * 200
  <>
  round(views_previous / 200) * 200
;