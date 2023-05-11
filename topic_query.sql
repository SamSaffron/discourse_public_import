-- [params]
-- int :min_id = 0

SELECT
    t.id,
    c.name,
    t.title,
    t.created_at,
    t.user_id,
    (SELECT STRING_AGG(tag.name, ', ') FROM topic_tags tt JOIN tags tag ON tag.id = tt.tag_id WHERE tt.topic_id = t.id) AS all_tags
FROM topics t
JOIN categories c ON c.id = t.category_id
WHERE NOT c.read_restricted AND t.deleted_at IS NULL
AND t.id > :min_id
ORDER BY t.id ASC
