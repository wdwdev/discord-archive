# Common Query Patterns

## Table of Contents
1. [User Profile Investigation](#user-profile-investigation)
2. [Server History](#server-history)
3. [Event / Incident Investigation](#event--incident-investigation)
4. [Image Viewing Pipeline](#image-viewing-pipeline)
5. [Useful SQL Techniques](#useful-sql-techniques)

---

## User Profile Investigation

Use when asked about a person's background, activity, or relationships.

**Step 1 — Find user and overall stats:**
```sql
SELECT u.user_id, u.username, u.global_name, u.bot,
       COUNT(m.message_id) AS msg_count,
       MIN(m.created_at) AS first_msg,
       MAX(m.created_at) AS last_msg,
       COUNT(DISTINCT m.channel_id) AS channels_used,
       COUNT(DISTINCT m.guild_id) AS guilds
FROM users u
JOIN messages m ON m.author_id = u.user_id
WHERE LOWER(u.username) LIKE '%name%'
   OR LOWER(u.global_name) LIKE '%name%'
GROUP BY u.user_id, u.username, u.global_name, u.bot
```

**Step 2 — Server and channel distribution (run in parallel):**
```sql
-- Per-server breakdown
SELECT g.guild_id, g.name, COUNT(*) AS msgs,
       MIN(m.created_at) AS first_msg, MAX(m.created_at) AS last_msg
FROM messages m JOIN guilds g ON g.guild_id = m.guild_id
WHERE m.author_id = :uid
GROUP BY g.guild_id, g.name ORDER BY msgs DESC

-- Top channels
SELECT c.name, g.name AS guild, COUNT(*) AS msgs
FROM messages m
JOIN channels c ON c.channel_id = m.channel_id
JOIN guilds g ON g.guild_id = m.guild_id
WHERE m.author_id = :uid
GROUP BY c.name, g.name ORDER BY msgs DESC LIMIT 15
```

**Step 3 — Personality & interests (run in parallel):**
```sql
-- Random sample of longer messages (captures voice/style)
SELECT content FROM messages
WHERE author_id = :uid AND content != '' AND length(content) > 30
ORDER BY random() LIMIT 40

-- Earliest messages (origin story)
SELECT content, created_at FROM messages
WHERE author_id = :uid AND content != ''
ORDER BY created_at ASC LIMIT 20
```

**Step 4 — How others see them:**
```sql
SELECT u.username, m.content, m.created_at
FROM messages m JOIN users u ON u.user_id = m.author_id
WHERE m.guild_id = :main_guild
  AND m.author_id != :uid
  AND LOWER(m.content) LIKE '%name%'
  AND length(m.content) > 20
ORDER BY random() LIMIT 25
```

**Step 5 — Semantic deep dive:**
```
semantic_search(query="name personal info country age school background",
                include_text=true, limit=15)
```

---

## Server History

Use when asked about a server's history, culture, or evolution.

**Step 1 — Server overview:**
```sql
SELECT guild_id, name, description, owner_id FROM guilds WHERE guild_id = :gid

SELECT COUNT(*) AS total_msgs, COUNT(DISTINCT author_id) AS unique_authors,
       MIN(created_at) AS first_msg, MAX(created_at) AS last_msg
FROM messages WHERE guild_id = :gid
```

**Step 2 — Channel structure and top contributors:**
```sql
-- Channel list
SELECT channel_id, name, topic, type FROM channels
WHERE guild_id = :gid ORDER BY type, position

-- Top posters
SELECT u.username, u.global_name, COUNT(*) AS msgs
FROM messages m JOIN users u ON u.user_id = m.author_id
WHERE m.guild_id = :gid
GROUP BY u.username, u.global_name ORDER BY msgs DESC LIMIT 20
```

**Step 3 — Activity over time (monthly):**
```sql
SELECT date_trunc('month', created_at) AS month,
       COUNT(*) AS msgs, COUNT(DISTINCT author_id) AS authors
FROM messages WHERE guild_id = :gid
GROUP BY month ORDER BY month
```

**Step 4 — Key events via semantic search:**
```
semantic_search(query="server created history important event",
                guild_id=:gid, include_text=true, limit=20)
```

---

## Event / Incident Investigation

Use when asked "why did X happen", "when was Y banned", etc.

**Step 1 — Keyword search for direct mentions:**
```sql
SELECT u.username, m.content, m.created_at, c.name AS channel
FROM messages m
JOIN users u ON u.user_id = m.author_id
JOIN channels c ON c.channel_id = m.channel_id
WHERE m.guild_id = :gid
  AND (LOWER(m.content) LIKE '%keyword1%' OR LOWER(m.content) LIKE '%keyword2%')
  AND length(m.content) > 15
ORDER BY m.created_at DESC LIMIT 30
```

**Step 2 — Semantic search for context:**
```
semantic_search(query="description of the event",
                guild_id=:gid, include_text=true, limit=20)
```

**Step 3 — Timeline reconstruction:**
Fetch messages around key timestamps to build chronological narrative:
```sql
SELECT u.username, m.content, m.created_at
FROM messages m JOIN users u ON u.user_id = m.author_id
WHERE m.channel_id = :cid
  AND m.created_at BETWEEN :start AND :end
ORDER BY m.created_at
LIMIT 100
```

---

## Image Viewing Pipeline

1. Find attachments:
   ```sql
   SELECT a.attachment_id, a.message_id, a.filename, a.content_type,
          a.width, a.height, a.size
   FROM attachments a
   WHERE a.message_id = :mid  -- or join with messages for broader queries
   ```

2. Refresh expired URL:
   ```
   refresh_attachment_url(message_id=:mid)
   ```

3. Download and view:
   ```bash
   curl -sL "REFRESHED_URL" -o /tmp/filename.ext
   ```
   Then use the `Read` tool on `/tmp/filename.ext` to view the image.

---

## Useful SQL Techniques

### Random sampling
```sql
ORDER BY random() LIMIT N
```
Use to get diverse, representative samples instead of biased chronological slices.

### Filter short message noise
```sql
WHERE content != '' AND length(content) > 30
```
Single-word messages and emoji-only messages are usually noise for profile analysis.

### Snowflake to creation date
```sql
SELECT to_timestamp(((user_id >> 22) + 1420070400000) / 1000.0) AS account_created
FROM users WHERE user_id = :uid
```

### Count distinct for activity metrics
```sql
COUNT(DISTINCT m.channel_id)  -- breadth of activity
COUNT(DISTINCT m.guild_id)    -- cross-server presence
COUNT(DISTINCT DATE(m.created_at))  -- active days
```

### Find messages mentioning a user (via mentions array)
```sql
SELECT * FROM messages WHERE :uid = ANY(mentions)
```
