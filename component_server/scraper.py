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
import pathlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import alembic.config
from alembic import command
from component_server.config import config
from component_server.factory import process
from component_server.jlcpcb_scraper import JlcpcbScraper
from component_server.models import Part, create_or_update_part

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
# 3. Brrrr...
categories = set()
categories_path = pathlib.Path("categories.txt")
categories_path.unlink(missing_ok=True)
categories_path.touch()

scraper = JlcpcbScraper(config.JLCPCB_KEY, config.JLCPCB_SECRET)
for i, part_data in enumerate(scraper.get_parts()):
    log.debug("Processing part %s", i)
    # Dump the categories to a file for reference
    category = (part_data["firstCategory"], part_data["secondCategory"])
    if category not in categories:
        categories.add(category)
        log.info("Adding category %s", category)
        with categories_path.open("a") as f:
            f.write(f"{category}\n")

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
