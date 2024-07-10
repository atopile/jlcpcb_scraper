from setuptools import setup, find_packages

setup(
    name='component_server',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'requests',
        'beautifulsoup4',
    ],
    entry_points={},
    url='https://github.com/atopile/component-server',
    license='MIT',
    author='Your Name',
    author_email='your.email@example.com',
    description='A Python module to scrape jlcpcb.com/parts for all available parts'
)
