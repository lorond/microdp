from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    kafka_bootstrap_servers: str = "redpanda:9092"
    clickstream_topic: str = "clickstream.events"
    clickstream_dlq_topic: str = "clickstream.events.dlq"


settings = Settings()
