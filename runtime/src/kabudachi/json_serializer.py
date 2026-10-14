"""Shared JSON shape and schema behavior for the JSON serializers."""

from typing import Any

from kabudachi import schemas
from kabudachi.errors import SerializationError


class JsonSerializer:
    """Common behavior for JSON backends that classify and build their own values."""

    classify: Any
    conform: Any

    def encode(self, value: Any, value_type: Any) -> bytes:
        """Encode one value, a list of values, or None as canonical JSON."""
        kind, item_type = self.classify(value_type)
        if kind is schemas.Shape.NOTHING:
            if value is not None:
                raise SerializationError(f"expected None, got {type(value).__name__}")
            return schemas.json_bytes(None)
        if kind is schemas.Shape.ONE:
            return schemas.json_bytes(self._plain(value, item_type))
        if not isinstance(value, list):
            raise SerializationError(f"expected a list, got {type(value).__name__}")
        return schemas.json_bytes([self._plain(item, item_type) for item in value])

    def decode(self, payload: bytes, target_type: Any) -> Any:
        """Decode one value, a list of values, or None from canonical JSON."""
        kind, item_type = self.classify(target_type)
        plain = schemas.parse_json(payload)
        if kind is schemas.Shape.NOTHING:
            if plain is not None:
                raise SerializationError("expected null for None")
            return None
        if kind is schemas.Shape.ONE:
            return self._build(plain, item_type, None)
        if not isinstance(plain, list):
            raise SerializationError("expected a JSON array for a list")
        return [self._build(item, item_type, index) for index, item in enumerate(plain)]

    def extract_version(self, payload: bytes, value_type: Any) -> int | None:
        kind, item_type = self.classify(value_type)
        path = schemas.version_path_of(item_type) if kind is schemas.Shape.ONE else None
        return schemas.json_version(payload, path)

    def schema_family(self, value_type: Any) -> str | None:
        return schemas.supported_family(self, value_type)

    def type_compatibility(self, produced: Any, consumed: Any) -> bool | None:
        return schemas.compatibility(self, produced, consumed)

    def load_plain(self, payload: bytes, value_type: Any) -> Any:
        return schemas.parse_json(payload)

    def dump_plain(self, plain: Any, value_type: Any) -> bytes:
        kind, item_type = self.classify(value_type)
        path = schemas.version_path_of(item_type) if kind is not schemas.Shape.NOTHING else None
        return schemas.dump_conformed(
            plain, kind, path, lambda item: self.conform(item, item_type)
        )

    def _plain(self, value: Any, item_type: Any) -> Any:
        raise NotImplementedError

    def _build(self, plain: Any, item_type: Any, index: int | None) -> Any:
        raise NotImplementedError
