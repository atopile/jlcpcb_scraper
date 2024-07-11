"""
Create JLCPCB models
"""

import datetime
import logging
from typing import Type, TypeVar

from component_server import parsers
from component_server.models import Capacitor, Inductor, Mosfet, Part, Resistor

T = TypeVar("T", bound="AbstractModelFactory")

log = logging.getLogger(__name__)


# Example data:
# {
#     'lcscPart': 'C1002',
#     'firstCategory': 'Filters',
#     'secondCategory': 'Ferrite Beads',
#     'mfrPart': 'GZ1608D601TF',
#     'solderJoint': '2',
#     'manufacturer': 'Sunlord',
#     'libraryType': 'base',
#     'description': '450mΩ ±25% 600Ω@100MHz 0603  Ferrite Beads ROHS',
#     'datasheet': 'https://wmsc.lcsc.com/wmsc/upload/file/pdf/v2/lcsc/2310301640_Sunlord-GZ1608D601TF_C1002.pdf',
#     'price': '20-180:0.004285714,200-780:0.003485714,1600-9580:0.002771429,800-1580:0.003042857,9600-19980:0.002542857,20000-:0.002414286',
#     'stock': 433437,
#     'package': '0603'
# }

# _known_footprints = {
# #     "01005": "C01005",
# #     "0201": "C0201",
# #     "0402": "C0402",
# #     "0504": "C0504",
# #     "0603": "C0603",
# #     "0805": "C0805",
# #     "1206": "C1206",
# #     "1210": "C1210",
# #     "1812": "C1812",
# #     "1825": "C1825",
# #     "2220": "C2220",
# #     "2225": "C2225",
# #     "3640": "C3640",
# #     "0201": "L0201",
# #     "0402": "L0402",
# #     "0603": "L0603",
# #     "0805": "L0805",
# #     "1206": "L1206",
# #     "1210": "L1210",
# #     "1806": "L1806",
# #     "1812": "L1812",
# #     "2010": "L2010",
# #     "2512": "L2512",
#     "01005": "R01005",
#     "0201": "R0201",
#     "0402": "R0402",
#     "0603": "R0603",
#     "0805": "R0805",
#     "1206": "R1206",
#     "1210": "R1210",
#     "2512": "R2512",
# }

class AbstractModelFactory:
    factories: list[Type["AbstractModelFactory"]] = []

    @classmethod
    def for_me(self, data: dict) -> bool:
        """Check if the data is for this factory"""
        raise NotImplementedError

    async def build(self, data: dict) -> Part | None:
        """Build a model from the data"""
        raise NotImplementedError

    @classmethod
    def register(cls) -> Type[T]:
        return cls.factories.append(cls)

    def _get_common(self, data: dict) -> dict | None:
        # FIXME: these are really daft criteria for the rating
        price = parsers.price(data.get("price"))
        stock = int(data.get("stock", 0))
        stock_cost = 20 if stock < 50 else 0
        rating = price + stock_cost

        return {
            "price": price,
            "stock": stock,
            "overhead_cost": 0,  # FIXME: we should find this data somewhere
            "rating": rating,
            "lcsc_id": data.get("lcscPart"),
            "mpn": data.get("mfrPart"),
            "package": data.get("package"),
            "last_update": datetime.datetime.now(datetime.UTC),
            # footprint_name isn't common because package -> footprint map
            #   is different for each component type
        }


class ResistorFactory(AbstractModelFactory):
    _known_footprints = {
        "01005": "R01005",
        "0201": "R0201",
        "0402": "R0402",
        "0603": "R0603",
        "0805": "R0805",
        "1206": "R1206",
        "1210": "R1210",
        "2512": "R2512",
    }

    @classmethod
    def for_me(cls, data: dict) -> bool:
        return (
            data.get("firstCategory") == "Resistors" and
            data.get("secondCategory") == "Chip Resistor - Surface Mount"
        )

    async def build(self, data: dict) -> Resistor | None:
        nominal_resistance = parsers.resistance(data.get("description"))
        if not nominal_resistance:
            # Handle both zero and None
            log.debug("Rejected because resistance couldn't be found")
            return
        tolerance_pct = parsers.percent(data.get("description"))
        if tolerance_pct is None:
            log.debug("Rejected because tolerance couldn't be found")
            return
        resistance_ohms_min = nominal_resistance * (1 - tolerance_pct / 100)
        resistance_ohms_max = nominal_resistance * (1 + tolerance_pct / 100)

        common = self._get_common(data)
        if not common:
            log.debug("Rejected because common data couldn't be found")
            return

        return Resistor(
            footprint_name = self._known_footprints.get(data.get("package")),
            resistance_ohms_min = resistance_ohms_min,
            resistance_ohms_max = resistance_ohms_max,
            operating_power_watts_min = 0,
            operating_power_watts_max = parsers.power(data.get("description")),
            operating_temp_celsius_min = None,  # TODO:
            operating_temp_celsius_max = None,  # TODO:
            **common
        )


ResistorFactory.register()


class CapacitorFactory(AbstractModelFactory):
    _known_footprints = {
        "01005": "C01005",
        "0201": "C0201",
        "0402": "C0402",
        "0504": "C0504",
        "0603": "C0603",
        "0805": "C0805",
        "1206": "C1206",
        "1210": "C1210",
        "1812": "C1812",
        "1825": "C1825",
        "2220": "C2220",
        "2225": "C2225",
        "3640": "C3640",
    }

    dielectric_min_temp = {
        "X": -55,
        "Y": -30,
        "Z": 10,
    }

    dielectric_max_temp = {
        "4": 65,
        "5": 85,
        "6": 105,
        "7": 125,
        "8": 150,
        "9": 200,
    }

    @classmethod
    def for_me(cls, data: dict) -> bool:
        return (
            data.get("firstCategory") == "Capacitors" and
            data.get("secondCategory") == "Multilayer Ceramic Capacitors MLCC - SMD/SMT"
        )

    async def build(self, data: dict) -> Capacitor | None:
        nominal_capacitance = parsers.capacitance(data.get("description"))
        if not nominal_capacitance:
            # Handle both zero and None
            log.debug("Rejected because capacitance couldn't be found")
            return
        tolerance_pct = parsers.percent(data.get("description"))
        if tolerance_pct is None:
            log.debug("Rejected because tolerance couldn't be found")
            return
        capacitance_farads_min = nominal_capacitance * (1 - tolerance_pct / 100)
        capacitance_farads_max = nominal_capacitance * (1 + tolerance_pct / 100)

        dielectric_code = parsers.dielectric(data.get("description"))
        if dielectric_code:
            operating_temp_celsius_min = self.dielectric_min_temp.get(dielectric_code[0])
            operating_temp_celsius_max = self.dielectric_max_temp.get(dielectric_code[1])
        else:
            operating_temp_celsius_min = None
            operating_temp_celsius_max = None

        common = self._get_common(data)
        if not common:
            log.debug("Rejected because common data couldn't be found")
            return

        rated_voltage = parsers.voltage(data.get("description"))
        if rated_voltage is None:
            operating_voltage_volts_min = None
            operating_voltage_volts_max = None
        else:
            operating_voltage_volts_min = -rated_voltage
            operating_voltage_volts_max = rated_voltage

        return Capacitor(
            footprint_name = self._known_footprints.get(data.get("package")),
            capacitance_farads_min=capacitance_farads_min,
            capacitance_farads_max=capacitance_farads_max,
            operating_voltage_volts_min=operating_voltage_volts_min,
            operating_voltage_volts_max=operating_voltage_volts_max,
            operating_temp_celsius_min=operating_temp_celsius_min,
            operating_temp_celsius_max=operating_temp_celsius_max,
            dielectric_code=dielectric_code,
            **common
        )


CapacitorFactory.register()


class InductorFactory(AbstractModelFactory):
    _known_footprints = {
        "0201": "L0201",
        "0402": "L0402",
        "0603": "L0603",
        "0805": "L0805",
        "1206": "L1206",
        "1210": "L1210",
        "1806": "L1806",
        "1812": "L1812",
        "2010": "L2010",
        "2512": "L2512",
    }

    @classmethod
    def for_me(cls, data: dict) -> bool:
        return (
            data.get("firstCategory") in [
                "Inductors & Chokes & Transformers",
                "Inductors, Coils, Chokes",
                "Inductors/Coils/Transformers"
            ] and
            data.get("secondCategory") in ["Power Inductors", "Inductors (SMD)"]
        )

    async def build(self, data: dict) -> Inductor | None:
        nominal_inductance = parsers.inductance(data.get("description"))
        if not nominal_inductance:
            # Handle both zero and None
            log.debug("Rejected because inductance couldn't be found")
            return
        tolerance_pct = parsers.percent(data.get("description"))
        if tolerance_pct is None:
            log.debug("Rejected because tolerance couldn't be found")
            return
        inductance_henries_min = nominal_inductance * (1 - tolerance_pct / 100)
        inductance_henries_max = nominal_inductance * (1 + tolerance_pct / 100)

        dc_resistance_max = dc_resistance_min = parsers.resistance(data.get("description"))

        rated_current = parsers.current(data.get("description"))
        if rated_current is None:
            operating_current_amps_min = None
            operating_current_amps_max = None
        else:
            operating_current_amps_min = -rated_current
            operating_current_amps_max = rated_current

        common = self._get_common(data)
        if not common:
            log.debug("Rejected because common data couldn't be found")
            return

        return Inductor(
            footprint_name = self._known_footprints.get(data.get("package")),
            inductance_henries_min=inductance_henries_min,
            inductance_henries_max=inductance_henries_max,
            dc_resistance_min=dc_resistance_min,
            dc_resistance_max=dc_resistance_max,
            operating_current_amps_min=operating_current_amps_min,
            operating_current_amps_max=operating_current_amps_max,
            operating_temp_celsius_min=None,
            operating_temp_celsius_max=None,
            **common
        )


InductorFactory.register()


class MosfetFactory(AbstractModelFactory):
    @classmethod
    def for_me(cls, data: dict) -> bool:
        category = (data.get("firstCategory"), data.get("secondCategory"))
        return category in [
            ('Transistors/Thyristors', 'MOSFETs'),
            ('Transistors', 'MOSFET'),
            ('Triode/MOS Tube/Transistor', 'MOSFETs'),
            ('Transistors', 'MOSFETs'),
        ]

    async def build(self, data: dict) -> Mosfet | None:
        operating_power_watts_min = 0
        operating_power_watts_max = parsers.power(data.get("description"))

        operating_voltage_volts_min = 0
        operating_voltage_volts_max = parsers.voltage(data.get("description"))

        operating_current_amps_min = 0
        operating_current_amps_max = parsers.current(data.get("description"))

        resistance, voltage, _ = parsers.mosfet_switching_specs(data.get("description"))
        gate_voltage_volts_min = voltage
        gate_voltage_volts_max = voltage

        on_resistance_ohms_min = resistance
        on_resistance_ohms_max = resistance

        common = self._get_common(data)
        if not common:
            log.debug("Rejected because common data couldn't be found")
            return

        return Mosfet(
            operating_voltage_volts_min=operating_voltage_volts_min,
            operating_voltage_volts_max=operating_voltage_volts_max,
            operating_current_amps_min=operating_current_amps_min,
            operating_current_amps_max=operating_current_amps_max,
            operating_power_watts_min=operating_power_watts_min,
            operating_power_watts_max=operating_power_watts_max,
            gate_voltage_volts_min=gate_voltage_volts_min,
            gate_voltage_volts_max=gate_voltage_volts_max,
            on_resistance_ohms_min=on_resistance_ohms_min,
            on_resistance_ohms_max=on_resistance_ohms_max,
            operating_temp_celsius_min=None,
            operating_temp_celsius_max=None,
            **common
        )


MosfetFactory.register()


async def process(data: dict) -> Part | None:
    for factory in AbstractModelFactory.factories:
        if factory.for_me(data):
            log.info(
                "%s accepted data in category %s-%s",
                factory.__class__.__name__,
                data.get("firstCategory"),
                data.get("secondCategory"),
            )

            component = await factory().build(data)
            if component:
                log.info("Built %s", component)
            else:
                log.info("Rejected")

            # Always return after the first factory that accepts the data
            return component
