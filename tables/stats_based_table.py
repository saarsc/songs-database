from typing import Any
from sqlalchemy.orm import relationship, declared_attr
from sqlalchemy import Column, Float, func, Numeric
from .base_table import BaseTable
from .song_entrie import SongEntrie
from operator import itemgetter
from functools import cached_property

class StatsBasedTable(BaseTable):
  __abstract__ = True

  @declared_attr
  def songs(self):
    return relationship("SongEntrie")

  @declared_attr
  def avg_danceability(self):
    return  Column("avg_danceability", Float)

  @declared_attr
  def avg_energy(self):
    return  Column("avg_energy", Float)

  @declared_attr
  def avg_key(self):
    return  Column("avg_key", Float)

  @declared_attr
  def avg_loudness(self):
    return  Column("avg_loudness", Float)

  @declared_attr
  def avg_mode(self):
    return  Column("avg_mode", Float)

  @declared_attr
  def avg_speechiness(self):
    return  Column("avg_speechiness", Float)

  @declared_attr
  def avg_acousticness(self):
    return  Column("avg_acousticness", Float)

  @declared_attr
  def avg_instrumentalness(self):
    return  Column("avg_instrumentalness", Float)

  @declared_attr
  def avg_liveness(self):
    return  Column("avg_liveness", Float)

  @declared_attr
  def avg_valence(self):
    return  Column("avg_valence", Float)

  @declared_attr
  def avg_tempo(self):
    return  Column("avg_tempo", Float)

  @declared_attr
  def avg_duration_ms(self):
    return  Column("avg_duration_ms", Float)

  @property
  def column_name(self) -> str:
    return self.table.__name__.lower()

  @cached_property
  def columns(self) -> list[str]:
    return [
      attr
      for attr in dir(self)
      if "avg_" in attr
    ]

  def calculate_stats(self):
    columns = []
    for col in self.columns:
      columns.append(
          func.round(
            func.cast(
                func.avg(getattr(SongEntrie, col.replace("avg_", ""))),
                Numeric(10, 2)
            ),
            2
          ).label(col)
      )

    stats = self.session.query(
      SongEntrie,
      *columns
    ).group_by(
      getattr(SongEntrie, f"{self.column_name}_id")
    ).all()

    for stat in stats:
      if table_column := getattr(stat[0], self.column_name):
        stat_values = zip(
          self.columns,
          itemgetter(*self.columns)(stat)
        )

        for attr, val in stat_values:
          setattr(table_column, attr, val)

    self.session.commit()

  def get_metadata(self) -> dict:
    return {
      "id": self.id,
      **{
        col: getattr(self, col)
        for col in self.columns
      }
    }

  def as_dict(self):
    return {
      **self.get_metadata()
    }
