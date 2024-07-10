
def test_voltage():
    from jlcpcb_scraper.parsers import voltage
    assert voltage("10V") == 10
    assert voltage("10V 20V") == 10
    assert voltage("20V 10V") == 20
    assert voltage("432mV") == 0.432


def test_dielectric():
    from jlcpcb_scraper.parsers import dielectric
    assert dielectric("asdasd asdas X5P") == "X5P"
    assert dielectric("12312kjnkj123 X7R") == "X7R"
    assert dielectric("X7R 10%") == "X7R"
    assert dielectric("X7R 10% 20V") == "X7R"
    assert dielectric("asdasX7R 10% 20V") is None
