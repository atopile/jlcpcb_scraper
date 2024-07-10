from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from alembic.config import Config
from alembic import command

from config import config

# 1. Run Alembic automatic revisions
alembic_cfg = Config("alembic.ini")
command.revision(alembic_cfg, autogenerate=True, message="Automatic revisions")

# 2. Run Alembic migrations on startup
command.upgrade(alembic_cfg, "head")

# 3. Get all current Category models from the database with sqlalchemy
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
