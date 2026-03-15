---
name: recall
description: >
  Discord Archive Recall — retrieve and analyze information from a Discord message
  archive stored in PostgreSQL + LanceDB. Use when the user asks about Discord users,
  servers, events, relationships, message history, or wants to view archived images/media.
  Triggers on: questions about people in Discord communities, server history, "who said X",
  "why did X happen", viewing memes/artwork/attachments, or any query prefixed with /recall.
---

# Discord Archive Recall

Retrieve information from a Discord message archive using `semantic_search` (vector similarity over NV-Embed-v2 embeddings in LanceDB) and `sql_query` (read-only PostgreSQL).

## Argument

$ARGUMENTS

## Tools

### `semantic_search`

Vector similarity search over message chunks.

- **Required**: `query` (natural language)
- **Key params**: `limit` (default 20), `guild_id`, `channel_id`, `author_id`, `after`/`before` (ISO datetime), `include_text` (boolean)
- **Always set `include_text: true`** to avoid extra SQL round-trips.
- **Distance**: lower = more similar. In practice, useful results range **1.0–1.4**. Below 1.0 is a strong match. Above 1.5 is usually noise.
- Works well for CJK queries — the model is multilingual.

### `sql_query`

Read-only SQL against PostgreSQL. Auto-appends `LIMIT 500` if none present.

- Schema: see [references/schema.md](references/schema.md)
- Column gotcha: the users table has `username` and `global_name` — there is no `name` column.

### `refresh_attachment_url`

Refresh expired Discord CDN signed URLs (expire ~24h).

- Params: `attachment_id` or `message_id` (provide one)
- Returns: JSON array with refreshed `url` field
- Pipeline: `sql_query` (find attachments) → `refresh_attachment_url` → `curl -sL URL -o /tmp/file` → `Read` tool to view

## Retrieval Strategy

### Multi-round approach

1. **Broad sweep** — Start with `semantic_search` (include_text=true, limit=20-30) and/or SQL aggregation queries to map the landscape.
2. **Parallel deep dives** — Run multiple queries concurrently: SQL for stats/keywords, semantic search for concepts. Maximize parallel tool calls.
3. **Targeted follow-up** — Based on findings, drill into specific channels, time ranges, or users.

### Query pattern selection

Match the query type to the right pattern. See [references/patterns.md](references/patterns.md) for full SQL templates:

| Query type | Pattern | Key technique |
|-----------|---------|---------------|
| Person's background | User Profile Investigation | Stats → distribution → random sample → others' mentions → semantic |
| Server history | Server History | Overview → channels → monthly activity → semantic events |
| Why did X happen | Event Investigation | Keyword search → semantic context → timeline reconstruction |
| View images/media | Image Viewing Pipeline | Find attachment → refresh URL → curl → Read |

### Practical tips

- **Sample, don't scan**: `ORDER BY random() LIMIT N` gives diverse results. Chronological ordering biases toward early/late messages.
- **Filter noise**: `WHERE content != '' AND length(content) > 30` removes emoji-only and single-word messages.
- **Snowflake dates**: `to_timestamp(((id >> 22) + 1420070400000) / 1000.0)` converts any Discord ID to its creation timestamp.
- **Mention lookup**: `WHERE :uid = ANY(mentions)` finds messages that @-mention a user (uses GIN index).
- **CJK substring**: `ILIKE '%keyword%'` — no word boundaries needed for CJK text.

## Output

Adapt format to the question. Use tables for stats, timelines for events, quotes for evidence. Respond in the user's language. Do not force a rigid template — let the content dictate the structure.
