import datetime
import operator

from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, Session, declarative_base

Base = declarative_base()


def in_(x: Column, y):
    return x.in_(y)


class Part(Base):
    """Abstract base of a part."""
    __abstract__ = True

    # Normal fields
    id = Column(Integer, primary_key=True)
    last_update = Column(DateTime, default=datetime.datetime.now(datetime.UTC))

    # Things we need to commonly update
    price         : Mapped[float] = Column(Float)
    stock         : Mapped[int]   = Column(Integer)
    overhead_cost : Mapped[float] = Column(Float)  # The cost of the setup, handling and shipping etc... as overhead of using this SKU
    rating        : Mapped[str]   = Column(Integer)  # Rating is a magic number representing stock, basic part status and cost. The higher the better.

    lcsc_id       : Mapped[str]   = Column(String, info={"return": True}, unique=True)
    mpn           : Mapped[str]   = Column(String, info={"return": True, "query_operator": in_})

    package       : Mapped[str]   = Column(String, info={"return": True, "query_operator": in_}, nullable=True)
    footprint_name: Mapped[str]   = Column(String, info={"return": True})


class Resistor(Part):
    """A model for a resistor part."""
    __tablename__ = "resistors"

    resistance_ohms_min: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.gt}, nullable=True)
    resistance_ohms_max: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.lt}, nullable=True)

    operating_power_watts_min: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.gt}, nullable=True)
    operating_power_watts_max: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.lt}, nullable=True)

    operating_temp_celsius_min: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.gt}, nullable=True)
    operating_temp_celsius_max: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.lt}, nullable=True)


class Capacitor(Part):
    """A model for a capacitor part."""
    __tablename__ = "capacitors"

    capacitance_farads_min: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.gt}, nullable=True)
    capacitance_farads_max: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.lt}, nullable=True)

    operating_voltage_volts_min: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.gt}, nullable=True)
    operating_voltage_volts_max: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.lt}, nullable=True)

    # Dielectric type information: rated temperature range and temperature variation
    # https://blog.knowlescapacitors.com/blog/simplify-capacitor-dielectric-selection-by-understanding-dielectric-coding-methods
    operating_temp_celsius_min: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.gt}, nullable=True)
    operating_temp_celsius_max: Mapped[float] = Column(Float, info={"return": True, "query_operator": operator.lt}, nullable=True)
    # TODO: figure out how we can encode tolerance based on temperature variation
    dielectric_code: Mapped[str] = Column(String)


def create_or_update_part(session: Session, comp: Part) -> Part:
    d = {k: v for k, v in comp.__dict__.items() if k in comp.__table__.columns.keys() and k != 'id'}
    stmt = insert(comp.__class__).values(**d).on_conflict_do_update(
        index_elements=['lcsc_id'],
        set_={
            'price': comp.price,
            'stock': comp.stock,
            'last_update': comp.last_update
        }
    )

    session.execute(stmt)
    return comp
