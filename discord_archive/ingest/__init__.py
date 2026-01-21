"""Discord Archive Ingest Pipeline.

This package provides tools for downloading Discord messages and metadata
into the PostgreSQL archive.

Usage:
    python -m discord_archive.ingest              # Process all guilds in config
    python -m discord_archive.ingest --guild-id X # Process specific guild
    python -m discord_archive.ingest --channel-id X # Process specific channel
"""
