import requests
import sqlite3
from pprint import pprint

base_url = "https://www.4olegacy.com/api/posts"
database = "posts.db"

create_tables = [
    """CREATE TABLE IF NOT EXISTS legacy_authors (
        id INTEGER PRIMARY KEY, 
        name text NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS legacy_categories (
        id INTEGER PRIMARY KEY, 
        name text NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS legacy_content_warnings (
        id INTEGER PRIMARY KEY, 
        name text NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS legacy_posts (
        id INTEGER PRIMARY KEY,
        post_id TEXT NOT NULL,
        title TEXT,
        commentary TEXT,
        created_at DATE,
        author_id INTEGER,
        FOREIGN KEY (author_id) REFERENCES legacy_authors (id)
    );""",
    """CREATE TABLE IF NOT EXISTS legacy_content (
        id INTEGER PRIMARY KEY,
        content TEXT,
        role TEXT,
        post_id INTEGER,
        FOREIGN KEY (post_id) REFERENCES legacy_posts (id)
    );""",
    """CREATE TABLE IF NOT EXISTS legacy_posts_categories (
        id INTEGER PRIMARY KEY,
        post_id INTEGER,
        category_id INTEGER,
        FOREIGN KEY (post_id) REFERENCES legacy_posts (id),
        FOREIGN KEY (category_id) REFERENCES legacy_categories (id)
    );""",
    """CREATE TABLE IF NOT EXISTS legacy_posts_warnings (
        id INTEGER PRIMARY KEY,
        post_id INTEGER,
        warning_id INTEGER,
        FOREIGN KEY (post_id) REFERENCES legacy_posts (id),
        FOREIGN KEY (warning_id) REFERENCES legacy_content_warnings (id)
    );""",
]

queries = {
    "posts_by_post_id": """SELECT * FROM legacy_posts WHERE post_id = ?""",
    "author_by_name": """SELECT * FROM legacy_authors WHERE name = ?""",
    "category_by_name": """SELECT * FROM legacy_categories WHERE name = ?""",
    "warnings_by_name": """SELECT * FROM legacy_content_warnings WHERE name = ?"""
}

inserts = {
    "posts": """INSERT INTO legacy_posts (post_id, title, commentary, created_at, author_id) VALUES (?,?,?,?,?);""",
    "authors": """INSERT INTO legacy_authors (name) VALUES (?);""",
    "content": """INSERT INTO legacy_content (content, role, post_id) VALUES (?,?,?)""",
    "categories": """INSERT INTO legacy_categories (name) VALUES (?);""",
    "content_warnings": """INSERT INTO legacy_content_warnings (name) VALUES (?);""",
    "posts_categories": """INSERT INTO legacy_posts_categories (post_id, category_id) VALUES (?,?);""",
    "posts_warnings": """INSERT INTO legacy_posts_warnings (post_id, warning_id) VALUES (?,?);"""
}

def process_post(post, cur):
    cur.execute(queries['posts_by_post_id'], (post['id'],))
    if cur.fetchone():
        print(f"Found duplicate: {post['id']}")
        return
    if not post["author"]:
        post["author"] = "Anonymous"
    cur.execute(queries['author_by_name'], (post["author"],))                                                                                                                                                                 
    author = cur.fetchone()                                                                                                                                                                                                   
    if not author:
        cur.execute(inserts["authors"], (post["author"],))
        author = cur.lastrowid   
    else:                                                                                                                                                                                                              
        author = author[0]
    cur.execute(inserts["posts"], (
      post["id"],                                                                                                                                                                                                           
      post["title"],                                                                                                                                                                                                        
      post.get("commentary"),
      post["createdAt"],
      author,
    ))
    pid = cur.lastrowid
    for c in post["chat"]:
        cur.execute(inserts["content"], (c["content"], c["role"], pid,))
    print(f"saved {post['id']}")
    for c in post["categories"]:
        cur.execute(queries['category_by_name'], (c,))
        if not cur.fetchone():
            cur.execute(inserts["categories"], (c,))
    for w in post.get("contentWarnings", []):
        cur.execute(queries['warnings_by_name'], (w,))
        if not cur.fetchone():
            cur.execute(inserts["content_warnings"], (w,))

try:
    with sqlite3.connect(database) as conn:
        cursor = conn.cursor()
        for i in create_tables:
            cursor.execute(i)   
        conn.commit()

except sqlite3.OperationalError as e:
    print(e)

try:
    r = requests.get(base_url)
    next_cursor = r.json()['pagination']['nextCursor']
    print(next_cursor)
    for post in r.json()['posts']:
        process_post(post, cursor)
        conn.commit()
    while next_cursor:
        r = requests.get(base_url, {"cursor": next_cursor})
        next_cursor = r.json()['pagination']['nextCursor']
        print(next_cursor)
        for post in r.json()['posts']:
            process_post(post, cursor)
            conn.commit()

except Exception as e: print(e)

print(r.json()['pagination'])
#pprint(r.json()['posts'][5])


