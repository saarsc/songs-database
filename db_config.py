from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base

conn = create_engine("sqlite:///songs.db", echo=False)
Base = declarative_base(conn)
session = sessionmaker(bind=conn)()
metadata = MetaData(bind=conn)

metadata.create_all()
