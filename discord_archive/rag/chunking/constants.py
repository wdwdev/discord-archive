"""Constants for the chunking module."""

# Discord message type for thread starter messages.
# These should be skipped during chunking as they're duplicates.
THREAD_STARTER_MESSAGE_TYPE = 21

# Maximum tokens per chunk (NV-Embed-v2 limit is 32768)
# Chunks exceeding this will be discarded to prevent embedding failures
MAX_CHUNK_TOKENS = 32768
