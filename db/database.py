import os
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.orm import DeclarativeBase
from dotenv import load_dotenv

load_dotenv()


class Base(DeclarativeBase):
    pass


def create_engine_for_process():
    url = os.environ["DATABASE_URL"]
    return _create_engine(url, pool_pre_ping=True)
