from datetime import datetime, timezone
from typing import Any

from sqlalchemy import BigInteger, DateTime, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator


class TZDateTime(TypeDecorator):
    """Timezone-aware datetime type that ensures UTC storage."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value

    def process_result_value(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


def utcnow() -> datetime:
    """Callable default for timezone-aware UTC timestamps."""
    return datetime.now(timezone.utc)


# Type aliases for Discord snowflakes and permission bitfields
Snowflake = BigInteger
PermissionBitfield = Numeric(precision=20, scale=0)


class Base(DeclarativeBase):
    """Declarative base for all Discord archive ORM models."""

    type_annotation_map = {
        int: BigInteger,
        dict: JSONB,
    }
