from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    max_workers: int = 16
    kafka_uri: str = 'localhost:9092'
    neo4j_uri: str = 'bolt://localhost:7687'
    neo4j_user: str = 'neo4j'
    neo4j_password: str = 'password'
