require "json"
require "net/http"
require "uri"
require "sqlite3"
require "mini_sql"
require "cgi"

begin
  config = JSON.parse(File.read("config.json"))
rescue StandardError
  puts "Please create a file called .creds with your API KEY and USERNAME"
end

# Replace these values with your Discourse instance details
DISCOURSE_DOMAIN = config["domain"]
API_KEY = config["api_key"]
API_USERNAME = config["api_username"]
TOPIC_QUERY_ID = config["topics_query_id"]
POST_QUERY_ID = config["posts_query_id"]
LIKES_QUERY_ID = config["likes_query_id"]

sqlite_conn = SQLite3::Database.new("dump.db")
conn = MiniSql::Connection.get(sqlite_conn)

def run_report(query_id:, min_id: 0, limit:)
  params = CGI.escape({ min_id: min_id.to_s }.to_json)

  uri =
    URI(
      "https://#{DISCOURSE_DOMAIN}/admin/plugins/explorer/queries/#{query_id}/run?limit=#{limit}&params=#{params}"
    )
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true

  request = Net::HTTP::Post.new(uri.request_uri)
  request["Content-Type"] = "application/json"
  request["Api-Key"] = API_KEY
  request["Api-Username"] = API_USERNAME

  response = http.request(request)
  if response.code != "200"
    puts "Error: #{response.code} #{response.message}"
    puts response.body
    exit 1
  end

  JSON.parse(response.body)
end

def create_schema(conn)
  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS topics (
      id INTEGER PRIMARY KEY,
      category,
      title,
      created_at,
      user_id,
      tags
    )
  SQL

  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS users(
      id INTEGER PRIMARY KEY,
      username,
      name
    )
  SQL

  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS posts(
      id INTEGER PRIMARY KEY,
      raw,
      post_number,
      topic_id,
      user_id,
      created_at
    )
  SQL

  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS likes(
      post_id,
      user_id,
      created_at
    )
  SQL

  conn.exec(
    "create unique index IF NOT EXISTS idxLikes on likes(post_id,user_id)"
  )

  conn.exec(
    "create index IF NOT EXISTS idxTopic on posts(topic_id,post_number)"
  )
end

def load_posts(conn, rows)
  highest_id = 0
  posts_loaded = 0

  conn.exec "BEGIN TRANSACTION"

  rows.each do |row|
    conn.exec <<~SQL, *row
    INSERT OR IGNORE INTO posts (id, raw, post_number, topic_id, user_id, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  SQL
    posts_loaded += 1
    highest_id = row[0] if row[0] > highest_id
  end

  conn.exec "COMMIT TRANSACTION"

  { highest_id: highest_id, posts_loaded: posts_loaded }
end

def load_topics(conn, rows)
  highest_id = 0
  topics_loaded = 0

  conn.exec "BEGIN TRANSACTION"

  rows.each do |row|
    conn.exec <<~SQL, *row
    INSERT OR IGNORE INTO topics (id, category, title, created_at, user_id, tags)
    VALUES (?, ?, ?, ?, ?, ?)
  SQL
    topics_loaded += 1
    highest_id = row[0] if row[0] > highest_id
  end

  conn.exec "COMMIT TRANSACTION"

  { highest_id: highest_id, topics_loaded: topics_loaded }
end

def load_users(conn, rows)
  conn.exec "BEGIN TRANSACTION"
  loaded = 0

  rows.each do |row|
    conn.exec <<~SQL, *row
    INSERT OR IGNORE INTO users(id, username, name)
    VALUES (?, ?, ?)
  SQL
    loaded += 1
  end

  conn.exec "COMMIT TRANSACTION"
  loaded
end

def load_users_from_json(conn, json)
  users = json.dig("relations", "user")
  if users
    users = users.map { |user| [user["id"], user["username"], user["name"]] }
    loaded = load_users(conn, users)
    puts "Loaded #{loaded} users"
  end
end

def load_likes(conn, json)
  result = { highest_id: 0, likes_loaded: 0 }

  conn.exec "BEGIN TRANSACTION"

  json["rows"].each do |row|
    conn.exec <<~SQL, *row
      -- id: ?
      INSERT OR IGNORE INTO likes(post_id, user_id, created_at)
      VALUES (?, ?, ?)
    SQL
    result[:highest_id] = row[0] if row[0] > result[:highest_id]
    result[:likes_loaded] += 1
  end

  conn.exec "COMMIT TRANSACTION"

  result
end

def download_topics(conn)
  min_id = 0
  while true
    response_data =
      run_report(query_id: TOPIC_QUERY_ID, min_id: min_id, limit: 10_000)

    load_users_from_json(conn, response_data)

    result = load_topics(conn, response_data["rows"])
    puts "Loaded #{result[:topics_loaded]} topics (highest id is #{result[:highest_id]})"

    min_id = result[:highest_id]
    break if result[:topics_loaded] == 0
  end
end

def download_posts(conn)
  min_id = 0
  while true
    response_data =
      run_report(query_id: POST_QUERY_ID, min_id: min_id, limit: 10_000)

    load_users_from_json(conn, response_data)

    result = load_posts(conn, response_data["rows"])
    puts "Loaded #{result[:posts_loaded]} posts (highest id is #{result[:highest_id]})"

    min_id = result[:highest_id]
    break if result[:posts_loaded] == 0
  end
end

def download_likes(conn)
  min_id = 0
  while true
    response_data =
      run_report(query_id: LIKES_QUERY_ID, min_id: min_id, limit: 10_000)

    result = load_likes(conn, response_data)

    puts "Loaded #{result[:likes_loaded]} likes (highest id is #{result[:highest_id]})"

    min_id = result[:highest_id]
    break if result[:likes_loaded] == 0
  end
end

create_schema(conn)
download_topics(conn)
download_posts(conn)
download_likes(conn)
