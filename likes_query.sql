-- [params]
-- int :min_id = 0

SELECT id, post_id pid, user_id uid, created_at
FROM post_actions
WHERE post_action_type_id = 2
AND deleted_at is NULL
AND id > :min_id
AND post_id IN (
  SELECT p.id
   FROM topics t
   JOIN posts p ON p.topic_id = t.id
   JOIN categories c ON c.id = t.category_id
   WHERE
    NOT c.read_restricted
     AND t.deleted_at IS NULL
     AND p.deleted_at IS NULL
     AND p.post_type = 1
     AND NOT p.hidden
)
ORDER BY id ASC

