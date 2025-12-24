###############################################################################
# database.py
#
# This module provides the database connection setup for the project using SQLAlchemy.
# It defines functions to create a database engine and a sessionmaker, allowing
# centralized and reusable access to the PostgreSQL database.
###############################################################################

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

DATABASE_URL = "postgresql://admin1:pass12345%40@db:5432/main"

def get_engine(echo: bool = True):
    """Create and return a SQLAlchemy engine."""
    return create_engine(
        DATABASE_URL,
        echo=echo,
        future=True
    )

def get_db_session():
    """
    Create and return a new SQLAlchemy Session bound to the engine.
    Use it as a context manager.
    """
    engine = get_engine()
    return Session(engine, future=True)