"""Tests for discord_archive.rag.embedding.model."""

from __future__ import annotations

from discord_archive.rag.embedding.model import EmbeddingModel, EmbeddingModelConfig


class TestEmbeddingModelConfig:
    """Tests for EmbeddingModelConfig."""

    def test_default_values(self) -> None:
        config = EmbeddingModelConfig()
        assert config.model_name == "nvidia/NV-Embed-v2"
        assert config.max_length == 32768
        assert config.batch_token_budget == 8_000

    def test_custom_values(self) -> None:
        config = EmbeddingModelConfig(
            model_name="custom/model",
            max_length=1024,
            batch_token_budget=10_000,
        )
        assert config.model_name == "custom/model"
        assert config.max_length == 1024
        assert config.batch_token_budget == 10_000


class TestEmbeddingModel:
    """Tests for EmbeddingModel."""

    def test_dimension_property(self) -> None:
        model = EmbeddingModel()
        assert model.dimension == 4096

    def test_dimension_constant(self) -> None:
        assert EmbeddingModel.DIMENSION == 4096

    def test_model_not_loaded_by_default(self) -> None:
        model = EmbeddingModel()
        assert model._model is None

    def test_custom_config(self) -> None:
        config = EmbeddingModelConfig(model_name="test/model")
        model = EmbeddingModel(config)
        assert model.config.model_name == "test/model"

    def test_encode_raises_without_load(self) -> None:
        model = EmbeddingModel()
        try:
            model.encode_documents(["test"])
            assert False, "Should have raised RuntimeError"
        except RuntimeError as e:
            assert "not loaded" in str(e)
