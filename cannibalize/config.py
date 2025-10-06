from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Settings:
    db_path: str = "cannibalize.db"
    min_impressions: int = 10
    min_urls_per_query: int = 2
    brand_terms: list[str] = field(default_factory=list)
    similarity_threshold: float = 0.3
    severity_weights: dict[str, float] = field(default_factory=lambda: {
        "volatility": 0.3,
        "click_dilution": 0.4,
        "impression_volume": 0.2,
        "similarity": 0.1,
    })
    ctr_curve: list[float] = field(default_factory=lambda: [
        0.319, 0.246, 0.185, 0.133, 0.095,
        0.065, 0.047, 0.035, 0.030, 0.026,
    ])

    @classmethod
    def load(cls, path: Path | None = None) -> Settings:
        config_path = path or Path("cannibalize.toml")
        if not config_path.exists():
            return cls()
        with open(config_path, "rb") as f:
            data = tomllib.load(f)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def save(self, path: Path | None = None) -> None:
        config_path = path or Path("cannibalize.toml")
        lines = []
        for key, val in self.__dict__.items():
            if isinstance(val, str):
                lines.append(f'{key} = "{val}"')
            elif isinstance(val, list):
                formatted = ", ".join(
                    f'"{v}"' if isinstance(v, str) else str(v) for v in val
                )
                lines.append(f"{key} = [{formatted}]")
            elif isinstance(val, dict):
                lines.append(f"\n[{key}]")
                for dk, dv in val.items():
                    lines.append(f'{dk} = {dv}')
            else:
                lines.append(f"{key} = {val}")
        config_path.write_text("\n".join(lines) + "\n")
