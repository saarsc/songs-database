from typing import Any
from sqlalchemy.orm import Session, Query, declared_attr
from sqlalchemy import Column, String, Integer

class BaseTable():
  __table_args__ = {
    'extend_existing': True,
  }

  __abstract__ = True

  def __init__(self, *args: Any, **kwargs: Any) -> None:
    if session := kwargs.get("session"):
      self.session: Session = session
      del kwargs["session"]

    super().__init__(*args, **kwargs)

  @declared_attr
  def id(self):
    return Column("id", Integer, primary_key=True)

  @declared_attr
  def name(self):
    return Column("name", String)

  @declared_attr
  def spotify_id(self):
    return Column("spotify_id", String)

  @property
  def table(self):
    return self.__class__

  @property
  def base_query(self) -> Query:
    return self.session.query(self.table)

  def insert_rows(self, rows: list) -> None:
    self.session.add_all(
      [self.table(**row.as_row()) for row in rows])
    self.session.commit()

  def insert_row(self, row) -> None:
    self.session.add(self.table(**row.as_row()))
    self.session.commit()

  def find_or_create_by_name(self, **kwargs):
    if instance := self.session.query(
        self.table
      ).filter_by(**kwargs).first():

      return instance

    instance = self.table(
      **kwargs
    )
    self.session.add(instance)
    return instance

  def get_all(self):
    return self.session.query(self.table).all()

  def by_id(self, id: str):
    return self.base_query.filter(self.table.id == id).first()

  def by_ids(self, ids):
    return self.base_query.filter(
      self.table.id.in_(list(ids))
    ).all()
