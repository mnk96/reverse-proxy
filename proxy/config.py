import logging
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class Address(BaseModel):
    """Модель для адреса, извлекаемая из строки 'host:port'."""
    host: str
    port: int

    @field_validator('port')
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError('Порт должен быть в диапазоне 1-65535')
        return v

class Timeouts(BaseModel):
    connect_ms: int = Field(default=1000, ge=0)
    read_ms: int = Field(default=15000, ge=0)
    write_ms: int = Field(default=15000, ge=0)
    total_ms: int = Field(default=30000, ge=0)

class Limits(BaseModel):
    max_client_conns: int = Field(default=1000, ge=1)
    max_conns_per_upstream: int = Field(default=100, ge=1)
    max_requests_per_conns: int = Field(default=100, ge=1)

class LoggingConfig(BaseModel):
    level: str = Field(default="info")

    @field_validator('level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        level = v.upper()
        if level not in logging._nameToLevel:
            raise ValueError(f"Неизвестный уровень логирования: {v}")
        return level


class Settings(BaseSettings):
    """Основная модель настроек, соответствующая YAML."""
    listen: Address
    metrics: Address
    upstreams: list
    timeouts: Timeouts = Field(default_factory=Timeouts)
    limits: Limits = Field(default_factory=Limits)
    max_size_conn: int = Field(default=10, ge=1)
    chunk_size: int = Field(default=1024, ge=1)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @model_validator(mode='before')
    @classmethod
    def preprocess_string_addresses(cls, values: dict) -> dict:
        """Преобразует строки listen и metrics в словари для Address."""
        for key in ('listen', 'metrics'):
            if key in values and isinstance(values[key], str):
                try:
                    host, port = values[key].split(':')
                    values[key] = {'host': host, 'port': int(port)}
                except ValueError:
                    raise ValueError(f"Неверный формат для {key}: ожидается 'host:port'")  # noqa: B904
        return values

    class Config:
        env_file = None
        extra = "forbid"

def load_settings() -> Settings:
    config_file = Path(__file__).parent.parent / 'config.yaml'
    if not config_file.exists():
        raise FileNotFoundError("Файл конфигурации не найден")
    with Path(config_file).open() as file:
        config = yaml.safe_load(file)
    return Settings(**config)

settings = load_settings()
