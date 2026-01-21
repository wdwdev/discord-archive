# Discord Archive

A Python-based tool for archiving Discord server data to PostgreSQL. Implements an ETL pipeline that ingests guild metadata, channels, roles, messages, emojis, stickers, and scheduled events using Discord's REST API with support for both historical backfill and incremental synchronization.

## Features

- **Multi-account support** - Archive from multiple Discord accounts with selective guild archiving
- **Full historical backfill** - Download all historical messages in a channel
- **Incremental sync** - Fetch only new messages since last run
- **Resumable operations** - Checkpoint system allows interruption and resumption
- **Permission-aware** - Automatically skips inaccessible channels to avoid API errors
- **Rate limit handling** - Respects Discord rate limits with automatic retry
- **Thread support** - Archives public and private archived threads
- **Rich logging** - Colored terminal output with progress tracking

## Architecture

```mermaid
graph TB
    subgraph CLI["CLI Layer"]
        A[python -m discord_archive.ingest]
    end

    subgraph Config["Configuration"]
        B[config.json]
    end

    subgraph Orchestration["Orchestration Layer"]
        D[IngestOrchestrator]
        E[GuildProcessor]
    end

    subgraph Ingestion["Ingestion Pipeline"]
        F[ChannelFetcher]
        G[EntityIngestor]
        H[Backfill]
        I[Incremental]
    end

    subgraph API["Discord API Layer"]
        J[DiscordClient]
    end

    subgraph DB["Database Layer"]
        K[Repositories]
        L[ORM Models]
        M[(PostgreSQL)]
    end

    A --> D
    B --> D
    D --> E
    E --> F
    E --> G
    E --> H
    E --> I
    F --> J
    G --> J
    H --> J
    I --> J
    J -->|REST API| N[Discord]
    H --> K
    I --> K
    G --> K
    K --> L
    L --> M
```

## Data Flow

```mermaid
sequenceDiagram
    participant CLI
    participant Orchestrator
    participant GuildProcessor
    participant DiscordClient
    participant Discord API
    participant Repository
    participant PostgreSQL

    CLI->>Orchestrator: run(config)
    loop For each account
        Orchestrator->>GuildProcessor: process_guild(guild_id)
        GuildProcessor->>DiscordClient: get_guild()
        DiscordClient->>Discord API: GET /guilds/{id}
        Discord API-->>DiscordClient: Guild JSON
        DiscordClient-->>GuildProcessor: Guild data
        GuildProcessor->>Repository: upsert_guild()
        Repository->>PostgreSQL: INSERT/UPDATE

        GuildProcessor->>DiscordClient: get_channels()
        DiscordClient->>Discord API: GET /guilds/{id}/channels
        Discord API-->>DiscordClient: Channels JSON

        loop For each text channel
            alt Backfill needed
                GuildProcessor->>DiscordClient: get_messages(before=oldest_id)
                DiscordClient->>Discord API: GET /channels/{id}/messages
                Discord API-->>DiscordClient: Messages batch
                GuildProcessor->>Repository: persist_messages_batch()
                Repository->>PostgreSQL: Bulk INSERT
            end

            GuildProcessor->>DiscordClient: get_messages(after=newest_id)
            DiscordClient->>Discord API: GET /channels/{id}/messages
            Discord API-->>DiscordClient: New messages
            GuildProcessor->>Repository: persist_messages_batch()
            Repository->>PostgreSQL: Bulk INSERT
        end
    end
    Orchestrator-->>CLI: Summary stats
```

## Installation

**Requirements:**
- Python >= 3.11
- PostgreSQL database

```bash
# Clone the repository
git clone https://github.com/yourusername/discord-archive.git
cd discord-archive

# Install with uv (recommended)
uv sync

# Or install with pip
pip install -e .
```

## Configuration

Create a `config.json` file (see `config.example.json` for reference):

```json
{
    "database_url": "postgresql+asyncpg://user:password@localhost:5432/discord_archive",
    "accounts": [
        {
            "name": "MyAccount",
            "token": "YOUR_DISCORD_TOKEN",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "guilds": ["123456789012345678", "987654321098765432"]
        }
    ]
}
```

### Configuration Options

| Field | Description |
|-------|-------------|
| `database_url` | PostgreSQL connection string using asyncpg driver |
| `accounts` | List of Discord account configurations |
| `accounts[].name` | Account identifier for logging |
| `accounts[].token` | Discord authorization token |
| `accounts[].user_agent` | HTTP User-Agent header |
| `accounts[].guilds` | List of guild IDs to archive (as strings) |

## Usage

```bash
# Archive all configured guilds
python -m discord_archive.ingest

# Archive a specific guild
python -m discord_archive.ingest --guild-id 123456789012345678

# Archive a specific channel
python -m discord_archive.ingest --channel-id 987654321098765432

# Use a custom config file
python -m discord_archive.ingest --config /path/to/config.json

# Enable verbose logging
python -m discord_archive.ingest --verbose

# Enable debug logging (includes third-party libraries)
python -m discord_archive.ingest --debug

# Log to file
python -m discord_archive.ingest --log-file archive.log
```

### CLI Options

| Option | Description |
|--------|-------------|
| `--config` | Path to config file (default: `config.json`) |
| `--guild-id` | Process only the specified guild |
| `--channel-id` | Process only the specified channel |
| `-v, --verbose` | Enable DEBUG logging for the application |
| `--debug` | Enable DEBUG logging including third-party libraries |
| `--log-file` | Write logs to specified file |

## Database Schema

```mermaid
erDiagram
    Guild ||--o{ Channel : contains
    Guild ||--o{ Role : contains
    Guild ||--o{ Emoji : contains
    Guild ||--o{ Sticker : contains
    Guild ||--o{ GuildScheduledEvent : contains
    Channel ||--o{ Message : contains
    Channel ||--o| IngestCheckpoint : tracks
    Message ||--o{ Attachment : contains
    Message ||--o{ Reaction : contains
    Message }o--o| User : references

    Guild {
        bigint guildId PK
        string name
        bigint ownerId
        string icon
        string description
        int verificationLevel
        int premiumTier
        array features
        jsonb raw
        timestamp archivedAt
    }

    Channel {
        bigint channelId PK
        bigint guildId FK
        bigint parentId FK
        int type
        string name
        string topic
        int position
        bool nsfw
        jsonb permissionOverwrites
        jsonb threadMetadata
        jsonb raw
        timestamp archivedAt
    }

    Message {
        bigint messageId PK
        bigint channelId FK
        bigint guildId
        bigint authorId
        string content
        int type
        int flags
        bool pinned
        timestamp createdAt
        timestamp editedTimestamp
        array mentions
        array mentionRoles
        jsonb embeds
        jsonb components
        jsonb poll
        jsonb raw
        timestamp archivedAt
    }

    User {
        bigint userId PK
        string username
        string discriminator
        string globalName
        string avatar
        bool bot
        bool system
        int publicFlags
        jsonb raw
        timestamp archivedAt
    }

    Role {
        bigint roleId PK
        bigint guildId FK
        string name
        int color
        bool hoist
        int position
        decimal permissions
        bool mentionable
        bool managed
        jsonb tags
        jsonb raw
        timestamp archivedAt
    }

    Attachment {
        bigint attachmentId PK
        bigint messageId FK
        string filename
        string contentType
        bigint size
        string url
        int width
        int height
        float durationSecs
        bool ephemeral
        jsonb raw
        timestamp archivedAt
    }

    Reaction {
        bigint messageId PK
        string emojiKey PK
        bigint emojiId
        string emojiName
        bool emojiAnimated
        int count
        jsonb countDetails
        jsonb raw
        timestamp archivedAt
    }

    Emoji {
        bigint emojiId PK
        bigint guildId FK
        string name
        bool animated
        bool available
        bool managed
        array roles
        bigint userId
        jsonb raw
        timestamp archivedAt
    }

    Sticker {
        bigint stickerId PK
        bigint guildId FK
        bigint packId
        string name
        string description
        string tags
        int type
        int formatType
        bool available
        jsonb raw
        timestamp archivedAt
    }

    GuildScheduledEvent {
        bigint eventId PK
        bigint guildId FK
        bigint channelId
        bigint creatorId
        string name
        string description
        int entityType
        int status
        int privacyLevel
        timestamp scheduledStartTime
        timestamp scheduledEndTime
        int userCount
        jsonb entityMetadata
        jsonb recurrenceRule
        jsonb raw
        timestamp archivedAt
    }

    IngestCheckpoint {
        bigint channelId PK
        bigint guildId
        bigint oldestMessageId
        bigint newestMessageId
        bool backfillComplete
        timestamp lastSyncedAt
        timestamp createdAt
    }
```

### Design Decisions

- **Append-only messages** - Messages are never deleted or updated (except `edited_timestamp`)
- **Latest-state snapshots** - Guild/channel/role metadata is overwritten on re-ingestion
- **JSONB fields** - Embeds, components, and polls stored as JSONB for forward compatibility
- **Checkpoint tracking** - Each channel tracks backfill progress for resumable operations
- **Denormalized guild_id** - Messages include `guild_id` to avoid joins for guild-wide queries

## Project Structure

```
discord_archive/
├── config/           # Configuration loading (Pydantic settings)
├── core/             # Base orchestrator infrastructure
├── db/
│   ├── models/       # SQLAlchemy ORM models
│   └── repositories/ # Data access layer
├── ingest/
│   ├── __main__.py   # CLI entry point
│   ├── run.py        # Main orchestration
│   ├── guild_processor.py
│   ├── backfill.py   # Historical message fetching
│   ├── incremental.py # New message sync
│   ├── channel_fetcher.py
│   ├── client.py     # Discord REST API client
│   ├── entity_ingestor.py
│   ├── mappers/      # JSON to ORM mappers
│   └── state.py      # Checkpoint management
└── utils/
    ├── permissions.py # Discord permission calculations
    ├── snowflake.py   # Discord ID utilities
    └── time.py        # Timestamp parsing
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Rate limit (429) | Wait for `Retry-After` duration, then retry |
| Server error (5xx) | Exponential backoff (1s to 64s), max 5 retries |
| Forbidden (403) | Skip channel, continue to next |
| Process crash | Resume from checkpoint on restart |

## Development

```bash
# Install with test dependencies
uv sync --extra test

# Run tests
pytest
```

## License

MIT License - see [LICENSE](LICENSE) for details.
