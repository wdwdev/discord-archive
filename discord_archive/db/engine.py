"""Database engine configuration.

Provides centralized database engine and session management.

Usage:
    from discord_archive.db.engine import get_engine, get_async_session

    # Get engine (cached per database_url)
    engine = get_engine()  # Uses settings.database_url
    engine = get_engine("postgresql+asyncpg://...")  # Custom URL

    # Get session factory
    AsyncSession = get_async_session()
    async with AsyncSession() as session:
        ...
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

if TYPE_CHECKING:
    pass


# Engine cache: database_url -> engine
_engine_cache: dict[str, AsyncEngine] = {}


def get_engine(database_url: str | None = None) -> AsyncEngine:
    """Get or create an async database engine.

    Engines are cached by database_url to avoid creating multiple
    connection pools for the same database.

    Args:
        database_url: Database connection URL. If None, uses the URL
                     from application settings.

    Returns:
        AsyncEngine instance (cached).
    """
    if database_url is None:
        from discord_archive.config.settings import get_settings

        database_url = get_settings().database_url

    if database_url not in _engine_cache:
        _engine_cache[database_url] = create_async_engine(
            database_url,
            echo=False,
            pool_pre_ping=True,  # Verify connections before use
        )

    return _engine_cache[database_url]


@lru_cache(maxsize=8)
def get_async_session(
    database_url: str | None = None,
) -> async_sessionmaker[AsyncSession]:
    """Get a session factory for the given database URL.

    Session factories are cached to ensure consistent configuration.

    Args:
        database_url: Database connection URL. If None, uses settings.

    Returns:
        async_sessionmaker instance for creating sessions.
    """
    engine = get_engine(database_url)
    return async_sessionmaker(bind=engine, expire_on_commit=False)


async def dispose_engines() -> None:
    """Dispose all cached engines.

    Call this during application shutdown to properly close
    all database connections.
    """
    for engine in _engine_cache.values():
        await engine.dispose()
    _engine_cache.clear()
    get_async_session.cache_clear()


# Backward compatibility aliases
def get_session_factory(
    database_url: str | None = None,
) -> async_sessionmaker[AsyncSession]:
    """Alias for get_async_session (backward compatibility)."""
    return get_async_session(database_url)
