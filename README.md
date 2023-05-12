### Public Data Dump for you forum

This repo attempts to establish a pattern for a public data dump. It includes 2 data explorer queries you can use to export all your public data.

Public data is defined as forum topics and posts that anonymous users can access.

### How to use this?

First you need to define 2 queries using data explorer:

1. Topic query: [here](topic_query.sql)
2. Post query: [here](post_query.sql)

Once defined note the data explorer query ids as specified in the URL

Next, define an API key with rights to run the 2 queries.

### config.json

Create a [config.json](config.json.sample) specifying the domain of your discourse site, api key and data explorer query ids.

### Importing the site into Sqlite

The first phase of the import is importing the site into a sqlite3 db. This intermediary db stores all the content.

Run: `ruby download_topics.rb`

### Importing the Sqlite db into Discourse

1. Start with a blank DB
2. Run `ruby import_db.rb`
