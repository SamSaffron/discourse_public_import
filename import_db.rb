require "sqlite3"
require "mini_sql"

sqlite_conn = SQLite3::Database.new("dump.db")
conn = MiniSql::Connection.get(sqlite_conn)

Dir.chdir("/home/sam/Source/discourse")
require "/home/sam/Source/discourse/config/environment"

puts "Importing users..."

created = 0
conn
  .query("SELECT * FROM users")
  .each do |row|
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
