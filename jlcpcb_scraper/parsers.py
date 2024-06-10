from functools import partial

_multipliers = {
    "p": 1.0e-12,
    "n": 1.0e-9,
    "u": 1.0e-6,
    "µ": 1.0e-6,  # duplicate ways to write micro
    "m": 1.0e-3,
    # skip x1
    "k": 1000.0,
    "M": 1000_000.0,
}

def _parse(unit: str, description: str) -> float | None:
    """Parse the capacitance from a component description"""
    if description is None:
        return None

    for part in description.split(' '):
        if len(part) < 2:
            continue

        if part[-1] != unit:
            continue

        if str.isdigit(part[-2]):
            multiplier = 1
            numeric_part = part[:-1]
        else:
            if part[-2] not in _multipliers:
                continue
            multiplier = _multipliers[part[-2]]
            numeric_part = part[:-2]

        try:
            return float(numeric_part) * multiplier
        except ValueError:
            pass

capacitance = partial(_parse, "F")
resistance = partial(_parse, "Ω")
inductance = partial(_parse, "H")
power = partial(_parse, "W")
current = partial(_parse, "A")
voltage = partial(_parse, "V")

def dielectric(description: str) -> str | None:
    """Parse the dielectric from a component description"""
    dielectric_value = None
    if description is None:
        return dielectric_value

    if "C0G" in description:
        dielectric_value = "C0G"
    elif "X7R" in description:
        dielectric_value = "X7R"
    elif "X5R" in description:
        dielectric_value = "X5R"
    elif "Y5V" in description:
        dielectric_value = "Y5V"

    return dielectric_value

def percent(description: str | None) -> float | None:
    """Parse the percentage from a component description"""
    if description is None:
        return None

    for fragment in description.split(' '):
        if fragment.endswith("%"):
            while not fragment[0].isdigit():
                fragment = fragment[1:]
            try:
                return float(fragment[:-1])
            except ValueError:
                pass

def price(price_description: str) -> float | None:
    """
    string input example: "'20-180:0.004285714,200-780:0.003485714,1600-9580:0.002771429,800-1580:0.003042857,9600-19980:0.002542857,20000-:0.002414286'"
    output example: 0.004285714
    """
    if not price_description:
        raise ValueError("Price is empty")

    price_groups = price_description.split(",")
    return float(price_groups[0].split(":")[1])
