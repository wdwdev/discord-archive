"""Token estimation for chunking.

Uses Mistral tokenizer for accurate token counting.
Requires the transformers package and network access on first run.
"""

from functools import lru_cache

from transformers import AutoTokenizer, PreTrainedTokenizerBase

# Model used for tokenization - must match embedding model's tokenizer
TOKENIZER_MODEL = "mistralai/Mistral-7B-v0.1"


class TokenizerLoadError(Exception):
    """Raised when the tokenizer cannot be loaded."""

    pass


@lru_cache(maxsize=1)
def get_tokenizer() -> PreTrainedTokenizerBase:
    """Get the Mistral tokenizer (cached).

    Raises:
        TokenizerLoadError: If the tokenizer cannot be loaded.
    """
    try:
        return AutoTokenizer.from_pretrained(TOKENIZER_MODEL)
    except Exception as e:
        raise TokenizerLoadError(
            f"Failed to load tokenizer '{TOKENIZER_MODEL}'. "
            f"Ensure 'transformers' is installed and you have network access "
            f"on first run. Error: {e}"
        ) from e


@lru_cache(maxsize=16384)
def estimate_tokens(text: str) -> int:
    """Count the number of tokens in a text using Mistral tokenizer.

    Results are cached (LRU, 16K entries) since messages are often
    processed multiple times during chunking.

    Raises:
        TokenizerLoadError: If the tokenizer cannot be loaded.
    """
    if not text:
        return 0
    return len(get_tokenizer().encode(text, add_special_tokens=False))
