# %% 
try:
    import IPython
except ImportError:
    print("IPython not found")
else:
    ipython = IPython.get_ipython()
    ipython.magic("load_ext autoreload")
    ipython.magic("autoreload 2")
    print("Enabled autoreload")


import logging
from jlcpcb_scraper.scraper import JlcpcbScraper

# %%
logging.basicConfig(level=logging.DEBUG)
logging.info("Starting JLCPCB scraper")


JLCPCB_KEY="app_key4699520"
JLCPCB_SECRET="app_secret4699520"

scraper = JlcpcbScraper(
    key=JLCPCB_KEY,
    secret=JLCPCB_SECRET
)

# %%
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from jlcpcb_scraper.models import Base, create_or_update_part

# Create an SQLite database and a session
engine = create_engine('sqlite:///example.db')
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# %%
from jlcpcb_scraper.model_factory import process
first_2000_parts = []

for i, part_data in enumerate(scraper.get_parts()):
    if i > 2000:
        break
    first_2000_parts.append(part_data)

# %%
for i, part_data in enumerate(first_2000_parts):
    logging.debug(f"Processing part {i}")
    if part := await process(part_data):
        logging.debug(f"Part {i} accepted")
        create_or_update_part(session, part)

session.commit()

# %%
from jlcpcb_scraper.models import Resistor, Capacitor

for i, p in enumerate(session.query(Resistor).all()):
    print(p.__dict__)

    if i > 100:
        break

for i, p in enumerate(session.query(Capacitor).all()):
    print(p.__dict__)

    if i > 100:
        break
# %%
