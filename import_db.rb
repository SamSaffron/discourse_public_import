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
  bang
  puts "creating likes..."

  created = 0
  conn
    .query("SELECT post_id,user_id,created_at FROM likes order by id asc")
    .each_slice(100) do |slice|
      PostAction.transaction do
        slice.each do |row|
          if DB.query(
               "SELECT 1 FROM post_actions where (post_id = ? and user_id = ? and post_action_type_id = ?)",
               row.post_id,
               row.user_id,
               2
             ).blank?
            p =
              PostAction.new(
                post_id: row.post_id,
                user_id: row.user_id,
                post_action_type_id: 2,
                created_at: row.created_at,
                updated_at: row.created_at
              )
            p.save!(validate: false)
            print "."
          end

          created += 1
          puts "#{created} likes created" if created % 500 == 0
        end
      end
    end
end

import_users(conn)
import_topics(conn)
import_posts(conn)
import_likes(conn)

Jobs::EnsureDbConsistency.new.execute(nil)
Topic.reset_all_highest!
