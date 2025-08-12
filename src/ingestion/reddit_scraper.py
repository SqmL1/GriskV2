"""
WSB PRAW Ingestion Script (SQLite)

What this does
---------------
- Connects to Reddit via PRAW (official Reddit API wrapper)
- Collects new submissions and full comment trees from r/wallstreetbets (configurable)
- Stores posts and comments into a local SQLite DB with raw JSON for re-parsing later
- Idempotent inserts (won't duplicate rows), with lightweight upserts on changing fields
- Simple retry/backoff + logging
- Can run once (cron-friendly) or loop on an interval for a lightweight daemon

Usage
-----
export REDDIT_CLIENT_ID=... \
       REDDIT_CLIENT_SECRET=... \
       REDDIT_USER_AGENT="wsb-pipeline/0.1 (by u/your_username)"

python wsb_praw_ingestor.py \
  --db data/wsb.db \
  --subreddit wallstreetbets \
  --max-posts 300 \
  --since-hours 24 \
  --interval-seconds 0   # 0 means run once; >0 to poll repeatedly

Notes
-----
- "since" filtering is best-effort: we walk subreddit.new() (reverse chronological)
  and stop once created_utc <= since_ts. Reddit may cap pagination to ~1000 items.
- For long historical backfills, consider adding Pushshift or a vendor later.
- This script only covers Step 1; ticker extraction comes next and will write to the
  `ticker_mentions` table already defined here.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

import praw
import prawcore

LOGGER = logging.getLogger("wsb_praw_ingestor")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

# ---------------------------
# DB Schema Helpers
# ---------------------------
POSTS_DDL = """
CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,
    title TEXT,
    selftext TEXT,
    author TEXT,
    score INTEGER,
    num_comments INTEGER,
    created_utc INTEGER,
    permalink TEXT,
    url TEXT,
    flair TEXT,
    stickied INTEGER,
    over_18 INTEGER,
    retrieved_utc INTEGER,
    raw_json TEXT
);
"""

COMMENTS_DDL = """
CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    post_id TEXT,
    parent_id TEXT,
    author TEXT,
    body TEXT,
    score INTEGER,
    created_utc INTEGER,
    depth INTEGER,
    retrieved_utc INTEGER,
    raw_json TEXT,
    FOREIGN KEY(post_id) REFERENCES posts(id)
);
"""

MENTIONS_DDL = """
CREATE TABLE IF NOT EXISTS ticker_mentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT,
    source_id TEXT,
    ticker TEXT,
    mention_time_utc INTEGER,
    retrieved_utc INTEGER,
    sentiment REAL,
    confidence REAL,
    context_excerpt TEXT,
    features_json TEXT
);
"""

INDEXES_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_utc);",
    "CREATE INDEX IF NOT EXISTS idx_comments_postid ON comments(post_id);",
    "CREATE INDEX IF NOT EXISTS idx_mentions_ticker_time ON ticker_mentions(ticker, mention_time_utc);",
]

UPSERT_POST = """
INSERT INTO posts (
    id, title, selftext, author, score, num_comments, created_utc, permalink,
    url, flair, stickied, over_18, retrieved_utc, raw_json
) VALUES (
    :id, :title, :selftext, :author, :score, :num_comments, :created_utc, :permalink,
    :url, :flair, :stickied, :over_18, :retrieved_utc, :raw_json
)
ON CONFLICT(id) DO UPDATE SET
    score=excluded.score,
    num_comments=excluded.num_comments,
    stickied=excluded.stickied,
    retrieved_utc=excluded.retrieved_utc,
    raw_json=excluded.raw_json;
"""

UPSERT_COMMENT = """
INSERT INTO comments (
    id, post_id, parent_id, author, body, score, created_utc, depth, retrieved_utc, raw_json
) VALUES (
    :id, :post_id, :parent_id, :author, :body, :score, :created_utc, :depth, :retrieved_utc, :raw_json
)
ON CONFLICT(id) DO UPDATE SET
    score=excluded.score,
    retrieved_utc=excluded.retrieved_utc,
    raw_json=excluded.raw_json;
"""


def utcnow_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(POSTS_DDL)
    conn.execute(COMMENTS_DDL)
    conn.execute(MENTIONS_DDL)
    for ddl in INDEXES_DDL:
        conn.execute(ddl)
    conn.commit()
    return conn


def get_last_post_ts(conn: sqlite3.Connection) -> Optional[int]:
    cur = conn.execute("SELECT MAX(created_utc) FROM posts")
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None

def praw_client() -> praw.Reddit:
    """Build a PRAW client from environment variables."""
    missing = [k for k in ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT") if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
        ratelimit_seconds=5,  # Be polite;
    )
    return reddit

def iter_new_submissions(
    reddit: praw.Reddit,
    subreddit_name: str,
    max_posts: int,
    since_ts: Optional[int],
) -> Iterable:
    """Yield new submissions from a subreddit, newest first, stopping at since_ts.

    PRAW's .new() is reverse-chronological. We'll stop when we hit posts older than since_ts.
    """
    sub = reddit.subreddit(subreddit_name)
    count = 0
    for s in sub.new(limit=max_posts):
        created = int(s.created_utc)
        if since_ts and created <= since_ts:
            break
        yield s
        count += 1
        if count >= max_posts:
            break

def store_submission(conn: sqlite3.Connection, s) -> None:
    data = {
        "id": s.id,
        "title": s.title or None,
        "selftext": s.selftext or None,
        "author": getattr(s.author, "name", None) if getattr(s, "author", None) else None,
        "score": int(s.score) if s.score is not None else None,
        "num_comments": int(s.num_comments) if s.num_comments is not None else None,
        "created_utc": int(s.created_utc),
        "permalink": s.permalink,
        "url": s.url if getattr(s, "url", None) else None,
        "flair": getattr(s, "link_flair_text", None),
        "stickied": 1 if getattr(s, "stickied", False) else 0,
        "over_18": 1 if getattr(s, "over_18", False) else 0,
        "retrieved_utc": utcnow_ts(),
        "raw_json": json.dumps(s.__dict__, default=str),
    }
    conn.execute(UPSERT_POST, data)

def store_comment(conn: sqlite3.Connection, c, post_id: str) -> None:
    data = {
        "id": c.id,
        "post_id": post_id,
        "parent_id": c.parent_id,
        "author": getattr(c.author, "name", None) if getattr(c, "author", None) else None,
        "body": c.body if hasattr(c, "body") else None,
        "score": int(c.score) if c.score is not None else None,
        "created_utc": int(c.created_utc),
        "depth": int(getattr(c, "depth", 0)),
        "retrieved_utc": utcnow_ts(),
        "raw_json": json.dumps(c.__dict__, default=str),
    }
    conn.execute(UPSERT_COMMENT, data)

def collect_post_and_comments(conn: sqlite3.Connection, submission) -> None:
    # Store submission first
    store_submission(conn, submission)

    # Expand and store comments
    try:
        submission.comments.replace_more(limit=0)
        for c in submission.comments.list():
            store_comment(conn, c, post_id=submission.id)
    except prawcore.exceptions.RequestException as e:
        LOGGER.warning("Comment fetch failed for %s: %s", submission.id, e)

def run_once(
    conn: sqlite3.Connection,
    reddit: praw.Reddit,
    subreddit: str,
    max_posts: int,
    since_hours: Optional[int],
) -> None:
    since_ts = None
    if since_hours is not None and since_hours > 0:
        since_ts = int((datetime.now(timezone.utc) - timedelta(hours=since_hours)).timestamp())
    else:
        # Default to last seen post in DB, if any
        since_ts = get_last_post_ts(conn)

    LOGGER.info(
        "Starting collection for r/%s | max_posts=%s | since_ts=%s",
        subreddit,
        max_posts,
        since_ts,
    )
    new_count = 0
    err_count = 0
    for s in iter_new_submissions(reddit, subreddit, max_posts=max_posts, since_ts=since_ts):
        try:
            collect_post_and_comments(conn, s)
            new_count += 1
            # Commit in small batches to keep WAL size in check
            if new_count % 10 == 0:
                conn.commit()
        except (prawcore.exceptions.Forbidden, prawcore.exceptions.NotFound) as e:
            LOGGER.warning("Skipping submission %s due to access issue: %s", s.id, e)
        except prawcore.exceptions.TooManyRequests as e:
            # Respect rate limits
            wait = getattr(e, "sleep_time", 10)
            LOGGER.warning("Rate limited. Sleeping %.1fs", wait)
            time.sleep(wait)
        except Exception as e:  # noqa: BLE001
            err_count += 1
            LOGGER.exception("Error processing submission %s: %s", getattr(s, "id", "?"), e)

    conn.commit()
    LOGGER.info("Done. New/updated submissions processed: %d | errors: %d", new_count, err_count)

def main():
    parser = argparse.ArgumentParser(description="WSB PRAW ingestor → SQLite")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file, e.g., data/wsb.db")
    parser.add_argument("--subreddit", default="wallstreetbets", help="Subreddit to ingest")
    parser.add_argument("--max-posts", type=int, default=300, help="Max posts to scan per run")
    parser.add_argument("--since-hours", type=int, default=None, help="Lookback hours (overrides DB last-seen)")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=0,
        help="If >0, poll repeatedly at this interval; otherwise run once",
    )

    args = parser.parse_args()

    conn = init_db(args.db)

    try:
        reddit = praw_client()
    except Exception as e:  # noqa: BLE001
        LOGGER.error("Failed to init PRAW client: %s", e)
        sys.exit(2)

    # Looping mode (simple daemon)
    if args.interval_seconds and args.interval_seconds > 0:
        LOGGER.info("Entering loop mode: every %ss", args.interval_seconds)
        while True:
            try:
                run_once(
                    conn=conn,
                    reddit=reddit,
                    subreddit=args.subreddit,
                    max_posts=args.max_posts,
                    since_hours=args.since_hours,
                )
            except prawcore.exceptions.ResponseException as e:
                LOGGER.warning("API response exception: %s", e)
            except Exception as e:  # noqa: BLE001
                LOGGER.exception("Unhandled error in loop: %s", e)
            finally:
                # light backoff between cycles
                time.sleep(args.interval_seconds)
    else:
        run_once(
            conn=conn,
            reddit=reddit,
            subreddit=args.subreddit,
            max_posts=args.max_posts,
            since_hours=args.since_hours,
        )

    conn.close()


if __name__ == "__main__":
    main()
