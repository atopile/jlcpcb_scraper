# %%
# 0. Enable autoreload for development and do imports
try:
    import IPython
except ImportError:
    print("IPython not found")
else:
    if ipython := IPython.get_ipython():
        ipython.run_line_magic("load_ext autoreload")
        ipython.run_line_magic("autoreload 2")
        print("Enabled autoreload")

import asyncio
import logging
from datetime import datetime, timedelta, UTC

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from alembic import command
from alembic.config import Config
from jlcpcb_scraper.config import config
from jlcpcb_scraper.models import Part, create_or_update_part
from jlcpcb_scraper.model_factory import process
from jlcpcb_scraper.scraper import JlcpcbScraper

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.info("Starting scraper")

# %%
# 1. Run Alembic automatic revisions
alembic_cfg = Config("alembic.ini")

# 2. Run Alembic migrations on startup
command.revision(alembic_cfg, autogenerate=True, message="Automatic revisions")
command.upgrade(alembic_cfg, "head")

# %%
# 3. Get all current Category models from the database with sqlalchemy
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

# 4. Initialize JLCPCB scraper with the current category models
scraper = JlcpcbScraper(config.JLCPCB_KEY, config.JLCPCB_SECRET)

# $$
# 5. Start scraping parts and update the database with new categories and parts
for i, part_data in enumerate(scraper.get_parts()):
    log.debug("Processing part %s", i)

    if part := asyncio.run(process(part_data)):
        log.debug("Part %s accepted", i)
        create_or_update_part(session, part)

    if i % 1000 == 0:
        log.info("Committing changes for %s parts to the database", i)
        session.commit()

# %%
# 6. Remove Parts older than 30 days from the database
print("Removing old parts from the database")
old_parts = session.query(Part).filter(Part.last_update < datetime.now(UTC) - timedelta(days=30)).all()
for part in old_parts:
    session.delete(part)
session.commit()
print(f"Removed { len(old_parts) } old parts from the database")

# Save changes to the database
session.commit()
session.close()