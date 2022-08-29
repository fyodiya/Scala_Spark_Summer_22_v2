#standardSQL
SELECT
  track_name,
  artist_name,
  user_name
FROM
  `listenbrainz.listenbrainz.listen`
  WHERE artist_name = 'David Bowie'
ORDER BY
  track_name DESC
LIMIT
  25