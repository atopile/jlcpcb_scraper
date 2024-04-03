from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from alembic.config import Config
from alembic import command

from parsers import resistance, capacitance, inductance, voltage, current, dielectric
from scraper import JlcpcbScraper
from models import Category, Part
from config import config


# 1. Run Alembic automatic revisions
alembic_cfg = Config("alembic.ini")
# command.revision(alembic_cfg, autogenerate=True, message="Automatic revisions")
# command.upgrade(alembic_cfg, "head")

# 2. Run Alembic migrations on startup
command.upgrade(alembic_cfg, "head")

# 3. Get all current Category models from the database with sqlalchemy
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

categories = session.query(Category).all()

# 4. Initialize JLCPCB scraper with the current category models
scraper = JlcpcbScraper(categories=categories)

# 5. Start scraping parts and update the database with new categories and parts
part_count = 0
categories_count = 0
for new_parts, new_categories in scraper.get_parts(session):
    # stop if no new parts or categories are found
    if new_parts is None and new_categories is None:
        break
    part_count += len(new_parts)
    categories_count += len(new_categories)
    session.commit()

print(f"Added { part_count } new parts and { categories_count } new categories to the database")

# 6. Remove Parts older than 30 days from the database
print("Removing old parts from the database")
old_parts = session.query(Part).filter(Part.last_update < datetime.utcnow() - timedelta(days=30)).all()
for part in old_parts:
    session.delete(part)
session.commit()
print(f"Removed { len(old_parts) } old parts from the database")

# 7. Iterate over all parts with a chuncked cursor
print("Parsing part descriptions")
for part in session.query(Part).yield_per(1000):
    # 8. Parse the description and update the part with the parsed values
    part.resistance = resistance.parse_resistance_description(part.description)
    part.capacitance = capacitance.parse_capacitance_description(part.description)
    part.inductance = inductance.parse_inductance_description(part.description)
    part.voltage = voltage.parse_voltage_description(part.description)
    part.current = current.parse_current_description(part.description)
    part.dielectric = dielectric.parse_dielectric_description(part.description)
    session.add(part)
session.commit()
print("Parsed part descriptions")


# Save changes to the database
session.commit()
session.close()