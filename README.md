# JLCPCB Scraper

This is a Python module to scrape parts information from jlcpcb.com/parts.

## Installation

To install the dependencies, run:

```bash
pip install -r requirements.txt
```

## Usage

Fill in the details in the .env file

### OR

Create a Postgres DB and make sure it is reachable by the pc that will execute this script.
Set environment variable SQLALCHEMY_DATABASE_URI, JLCPCB_KEY and JLCPCB_SECRET
```
export SQLALCHEMY_DATABASE_URI=postgres://YourUserName:YourPassword@YourHostname:5432/YourDatabaseName
export JLCPCB_KEY=<KEY>
export JLCPCB_SECRET=<SECRET>
```

Execute main.py
```
python3 jlcpcb_scraper/main.py
```


## Testing TODO, INCOMPLETE

To run the tests, use the following command:

```bash
pytest tests
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)

## Creating a DB

You need Postgres running; eg. `docker up` from the component server project

Alembic won't create a DB, you need to do that manually from Postgres Admin or the likes. Name is `atopile-components` or something like that.

I've had issues with alembic creating tables from scratch? Perhaps I had things in a dirty state.

```python
from sqlalchemy import create_engine

from jlcpcb_scraper.config import config
from jlcpcb_scraper.models import Base

engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
Base.metadata.create_all(engine)
```

## Local developement

### Running a local postgreSQL database

Make sure docker is running and run `docker-compose up`.

### Updating the alembic database schema

If you change the database schema, use the following command to update the alembic version and migration script: `alembic revision --autogenerate -m "explain what happened"`

To apply the changes to the database, run `alembic upgrade head`. Your database should now follow the alchemy ORM schema.

### Inspecting your database

To inspect the contents of your database and make changes manually, use the `psql` util. `psql` can be installed with `brew` on mac with `brew install postgresql@16`.

```bash
psql -U atopile -h localhost -p 5432 -d atopile-components
```

To list the tables, invoke `\dt`. To quit, invoke `exit`. To delete tables we are not using anymore, invoke `DROP TABLE public.alembic_version, public.ranged_values;

### Starting the server

1. Run `docker-compose up` to start PostgreSQL.
2. Run `fastapi dev endpoints.py` to start the FastAPI server.


## Up-revving database

1. Upgrade the schema
2. Create a revision `alembic revision --autogenerate -m "Why?"`
3. Run the upgrade on the `alembic upgrade head`
