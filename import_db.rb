require "sqlite3"
require "mini_sql"

sqlite_conn = SQLite3::Database.new("dump.db")
conn = MiniSql::Connection.get(sqlite_conn)

Dir.chdir("/home/sam/Source/discourse")
require "/home/sam/Source/discourse/config/environment"

RateLimiter.disable

def import_users(conn)
  puts "Importing users..."
  created = 0
  conn
    .query("SELECT * FROM users")
    .each_slice(5000) do |slice|
      slice.each do |row|
        User.transaction do
          if !User.exists?(row.id)
            User.create(
              id: row.id,
              username: row.username,
              name: row.username,
              password: SecureRandom.hex,
              email: "#{SecureRandom.hex}@email.com"
            )
          end
          print "."
          created += 1
          puts "#{created} users created" if created % 500 == 0
        end
      end
    end
end

def import_topics(conn)
  categories =
    Category.pluck(:name, :id).map { |name, id| [name.downcase, id] }.to_h

  puts "ensuring categories exist..."
  # ensuring categories exist
  conn
    .query("SELECT DISTINCT category FROM topics")
    .each do |row|
      if !categories[row.category.downcase]
        category =
          Category.create!(
            name: row.category,
            user_id: -1,
            skip_category_definition: true
          )
        categories[row.category.downcase] = category.id
        puts "created #{row.category}"
      end
    end

  puts "creating topics..."

  created = 0
  conn
    .query("SELECT id FROM topics")
    .each_slice(100) do |slice|
      Topic.transaction do
        slice.each do |row|
          if !Topic.exists?(row.id)
            topic =
              conn.query("SELECT * FROM topics WHERE id = ?", row.id).first
            t =
              Topic.new(
                id: topic.id,
                title: topic.title,
                category_id: categories[topic.category],
                user_id: topic.user_id,
                created_at: topic.created_at,
                updated_at: topic.created_at
              )
            t.save!(validate: false)
            print "."
          end

          created += 1
          puts "#{created} topics created" if created % 500 == 0
        end
      end
    end
end

def import_posts(conn)
  puts "creating posts..."

  created = 0
  conn
    .query("SELECT id,topic_id,post_number FROM posts order by id asc")
    .each_slice(100) do |slice|
      Post.transaction do
        slice.each do |row|
          if DB.query(
               "SELECT 1 FROM posts where (topic_id = ? and post_number = ?)",
               row.topic_id,
               row.post_number
             ).blank?
            post = conn.query("SELECT * FROM posts WHERE id = ?", row.id).first

            p =
              Post.new(
                raw: post.raw,
                cooked: PrettyText.cook(post.raw),
                user_id: post.user_id,
                created_at: post.created_at,
                updated_at: post.created_at,
                post_number: post.post_number,
                topic_id: post.topic_id
              )
            p.save!(validate: false)
            print "."
          end

          created += 1
          puts "#{created} posts created" if created % 500 == 0
        end
      end
    end
end

def import_likes(conn)
  puts "creating likes..."

  created = 0
  conn
    .query(
      "SELECT post_id,user_id,created_at FROM likes order by post_id, user_id asc"
    )
    .each_slice(100) do |slice|
      PostAction.transaction do
        slice.each do |row|
          if DB.query(
               "SELECT 1 FROM post_actions where (post_id = ? and user_id = ? and post_action_type_id = ?)",
               row.post_id,
               row.user_id,
               2
             ).blank?
            DB.exec(
              "INSERT INTO post_actions (post_id, user_id, post_action_type_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
              row.post_id,
              row.user_id,
              2,
              row.created_at,
              row.created_at
            )
            print "."
          end

          created += 1
          puts "#{created} likes created" if created % 500 == 0
        end
      end
    end
end

def fix_likes
  DB.exec <<~SQL
  INSERT INTO user_actions (
    user_id,
    action_type,
    target_topic_id,
    target_post_id,
    acting_user_id,
    created_at,
    updated_at
  )
  SELECT pa.user_id, 1, p.topic_id, pa.post_id, pa.user_id, pa.created_at, pa.created_at
  FROM post_actions pa
  JOIN posts p ON p.id = pa.post_id
  WHERE post_action_type_id = 2
  UNION ALL
  SELECT p.user_id, 2, p.topic_id, pa.post_id, pa.user_id, pa.created_at, pa.created_at
  FROM post_actions pa
  JOIN posts p ON p.id = pa.post_id
  WHERE post_action_type_id = 2
  ON CONFLICT DO NOTHING
  SQL
end

def fix_sequences
  %w[users topics posts post_actions].each { |table| DB.exec <<~SQL }
    SELECT setval('public."#{table}_id_seq"',
      (SELECT MAX(id) FROM public.#{table})
    );
  SQL
end

def fix_counts
  DB.exec <<~SQL
   UPDATE posts
   SET like_count = (
    SELECT COUNT(*)
    FROM post_actions
    WHERE post_actions.post_id = posts.id
    AND post_action_type_id = 2
   )
  SQL

  DB.exec <<~SQL
   UPDATE topics
   SET like_count = (
    SELECT SUM(like_count)
    FROM posts
    WHERE posts.topic_id = topics.id
    )
  SQL
end

def fix_dates
  DB.exec <<~SQL
    UPDATE topics
    SET created_at = (SELECT MIN(created_at) FROM posts WHERE posts.topic_id = topics.id)
  SQL

  DB.exec <<~SQL
    UPDATE topics
    SET updated_at = (SELECT MAX(created_at) FROM posts WHERE posts.topic_id = topics.id)
  SQL

  DB.exec <<~SQL
    UPDATE topics
    SET bumped_at = (SELECT MAX(created_at) FROM posts WHERE posts.topic_id = topics.id)
  SQL
end

import_users(conn)
import_topics(conn)
import_posts(conn)
import_likes(conn)
fix_likes
fix_sequences
fix_counts
fix_dates

Jobs::EnsureDbConsistency.new.execute(nil)
Topic.reset_all_highest!
