# %%
# 0. Enable autoreload for development and do imports
try:
    import IPython
except ImportError:
    print("IPython not found")
else:
    if ipython := IPython.get_ipython():
        ipython.magic("load_ext autoreload")
        ipython.magic("autoreload 2")
        print("Enabled autoreload")

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import alembic.config
from alembic import command
from jlcpcb_scraper.config import config
from jlcpcb_scraper.factory import process
from jlcpcb_scraper.models import Part, create_or_update_part
from jlcpcb_scraper.scraper import JlcpcbScraper


# %%
# 1. Manage db schema with alembic
# Revision this manually with `alembic revision --autogenerate -m "My message"`
alembic_cfg = alembic.config.Config("alembic.ini")
# This also configures the base logger from the alembic
command.upgrade(alembic_cfg, "head")
log = logging.getLogger(__name__)
log.info("Starting scraper")


# %%
# 2. Create a database session, ensuring the tables structure exists
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()


# %%
# 3. Initialize JLCPCB scraper with the current category models
scraper = JlcpcbScraper(config.JLCPCB_KEY, config.JLCPCB_SECRET)
for i, part_data in enumerate(scraper.get_parts()):
    log.debug("Processing part %s", i)

    if part := asyncio.run(process(part_data)):
        log.debug("Part %s accepted", i)
        create_or_update_part(session, part)

    if i % 1000 == 0:
        log.info("Committing changes for %s parts to the database", i)
        session.commit()


# %%
# 4. Remove Parts older than 30 days from the database
print("Removing old parts from the database")
old_parts = session.query(Part).filter(Part.last_update < datetime.now(UTC) - timedelta(days=30)).all()
for part in old_parts:
    session.delete(part)
session.commit()
print(f"Removed { len(old_parts) } old parts from the database")

# Clean up
session.close()
