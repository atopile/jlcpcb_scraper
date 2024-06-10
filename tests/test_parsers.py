
def test_voltage():
    from jlcpcb_scraper.parsers import voltage
    assert voltage("10V") == 10
    assert voltage("10V 20V") == 10
    assert voltage("20V 10V") == 20
    assert voltage("432mV") == 0.432
