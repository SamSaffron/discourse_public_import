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

  puts "creating topics..."

  created = 0
  conn
    .query("SELECT id FROM topics")
    .each_slice(100) do |slice|
      Topic.transaction do
        slice.each do |row|
          if !Topic.exists?(row.id)
            topic =
              conn.query(
                "SELECT * FROM topics t JOIN posts p on p.topic_id = t.id WHERE t.id = ?",
                row.id
              ).first

            if !categories[topic.category.downcase]
              category = Category.create!(name: topic.category, user_id: -1)
              categories[topic.category.downcase] = category.id
              puts "created #{topic.category}"
            end

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
# import_users(conn)
import_topics(conn)
#
