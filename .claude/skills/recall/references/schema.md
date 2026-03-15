# Database Schema

## Core Tables

### guilds
| Column | Type | Notes |
|--------|------|-------|
| guild_id | bigint PK | Discord snowflake |
| name | text | |
| description | text | |
| owner_id | bigint | |
| icon | text | |
| archived_at | timestamptz | |

### channels
| Column | Type | Notes |
|--------|------|-------|
| channel_id | bigint PK | |
| guild_id | bigint FK | |
| type | int | 0=text, 2=voice, 4=category, 5=announcement, 10-12=threads, 13=stage, 15=forum, 16=media |
| name | text | |
| topic | text | |
| parent_id | bigint | Category or parent channel |
| position | int | |

### users
| Column | Type | Notes |
|--------|------|-------|
| user_id | bigint PK | |
| username | text | Current username (NOT `name`) |
| global_name | text | Display name |
| discriminator | text | Legacy, usually "0" |
| bot | boolean | |
| avatar | text | |
| public_flags | int | |

### messages
| Column | Type | Notes |
|--------|------|-------|
| message_id | bigint PK | Snowflake — encodes creation timestamp |
| channel_id | bigint FK | |
| author_id | bigint | Soft FK to users (use LEFT JOIN) |
| guild_id | bigint | Denormalized |
| content | text | |
| created_at | timestamptz | |
| edited_timestamp | timestamptz | |
| type | int | 0=default, 19=reply |
| mentions | int8[] | Mentioned user IDs |
| mention_roles | int8[] | |
| pinned | boolean | |
| embeds | jsonb | |

### attachments
| Column | Type | Notes |
|--------|------|-------|
| attachment_id | bigint PK | |
| message_id | bigint FK | |
| filename | text | |
| content_type | text | e.g. image/png |
| size | bigint | Bytes |
| url | text | Signed CDN URL — expires ~24h |
| proxy_url | text | |
| description | text | Alt text |
| width | int | Images/video only |
| height | int | |
| duration_secs | float | Audio/video only |
| archived_at | timestamptz | |

### reactions
| Column | Type | Notes |
|--------|------|-------|
| message_id | bigint | Composite PK |
| emoji_key | text | Composite PK |
| emoji_name | text | |
| count | int | |

### roles
| Column | Type | Notes |
|--------|------|-------|
| role_id | bigint PK | |
| guild_id | bigint FK | |
| name | text | |
| color | int | |
| position | int | |
| permissions | bigint | |

## RAG Tables

### chunks
| Column | Type | Notes |
|--------|------|-------|
| chunk_id | serial PK | |
| chunk_type | text | 'reply_chain', 'author_group', 'sliding_window' |
| guild_id | bigint | |
| channel_id | bigint | |
| message_ids | int8[] | |
| author_ids | int8[] | |
| mentioned_user_ids | int8[] | |
| mentioned_role_ids | int8[] | |
| has_attachments | boolean | |
| first_message_at | timestamptz | |
| last_message_at | timestamptz | |

### chunk_texts
| Column | Type | Notes |
|--------|------|-------|
| chunk_id | int PK/FK | 1:1 with chunks |
| text | text | Formatted as `[username] date\nmessage` |
| token_count | int | |

## Key Indexes

- `messages`: on `(channel_id, created_at)`, `(guild_id, created_at)`, `author_id`, GIN on `mentions`
- `chunks`: on `channel_id`

## Snowflake to Timestamp

Discord snowflake IDs encode creation time: `timestamp_ms = (snowflake >> 22) + 1420070400000`. Use this to estimate account creation dates without needing external API calls.
