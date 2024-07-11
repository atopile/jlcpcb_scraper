from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from component_server.config import config

if "postgresql" in config.SQLALCHEMY_DATABASE_URI:
    engine = create_engine(
        config.SQLALCHEMY_DATABASE_URI,
        # pool_size=10,
        # pool_pre_ping=True,
        # executemany_values_page_size=50000,
        # executemany_batch_page_size=2000,
        # pool_timeout=30,  # add pool_timeout parameter
        # max_overflow=10,  # increase max_overflow parameter
        # pool_recycle=600,  # add pool_recycle parameter
        # pool_use_lifo=True,
    )
else:
    engine = create_engine(
        config.SQLALCHEMY_DATABASE_URI,
        pool_pre_ping=True,
        connect_args={"check_same_thread": False},
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)
