import os
from dotenv import load_dotenv

class Config:
    def __init__(self):
        load_dotenv()  # Load variables from .env file

        # Define your variables here
        self.SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI')
        self.POSTGRES_USER = os.getenv('POSTGRES_USER')
        self.POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
        self.POSTGRES_DB = os.getenv('POSTGRES_DB')
        self.JLCPCB_KEY = os.getenv('JLCPCB_KEY')
        self.JLCPCB_SECRET = os.getenv('JLCPCB_SECRET')


config = Config()
