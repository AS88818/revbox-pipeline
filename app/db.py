"""
Database session and engine management.
"""
import logging
from pathlib import Path
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from app.models import Base

logger = logging.getLogger(__name__)

_engine = None
_SessionFactory = None


def get_engine(db_path: str = "revbox_data.db"):
    """Return a singleton SQLAlchemy engine."""
    global _engine
    if _engine is None:
        db_url = f"sqlite:///{db_path}"
        _engine = create_engine(db_url, connect_args={"check_same_thread": False})

        # Enable WAL mode for better concurrent read performance
        @event.listens_for(_engine, "connect")
        def set_wal(dbapi_conn, _):
            dbapi_conn.execute("PRAGMA journal_mode=WAL")
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

        logger.info(f"Database engine initialised: {db_url}")
    return _engine


def init_db(db_path: str = "revbox_data.db") -> None:
    """Create all tables if they don't already exist."""
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    logger.info("Database schema initialised.")


def get_session(db_path: str = "revbox_data.db") -> Session:
    """Return a new session. Caller is responsible for closing."""
    global _SessionFactory
    if _SessionFactory is None:
        engine = get_engine(db_path)
        _SessionFactory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return _SessionFactory()
