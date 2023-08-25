from sqlalchemy.orm import relationship
from sqlalchemy import Column, Float, String, Integer, ForeignKey
from .base_table import BaseTable
from ..db_config import Base

class SongEntrie(BaseTable, Base):
  __tablename__ = "songs"

  artist_id = Column(Integer, ForeignKey("artist.id"))

  album_id = Column(Integer, ForeignKey("album.id"))

  artist = relationship("Artist", back_populates="songs")
  album = relationship("Album", back_populates="songs")

  song_key = Column("song_key", String)
  danceability = Column("danceability", Float)
  energy = Column("energy", Float)
  key = Column("key", Float)
  loudness = Column("loudness", Float)
  mode = Column("mode", Float)
  speechiness = Column("speechiness", Float)
  acousticness = Column("acousticness", Float)
  instrumentalness = Column("instrumentalness", Float)
  liveness = Column("liveness", Float)
  valence = Column("valence", Float)
  tempo = Column("tempo", Float)
  duration_ms = Column("duration_ms", Float)
  time_signature = Column("time_signature", Float)


  def by_key(self, key):
    return self.base_query.filter(self.table.song_key == key).first()

  def by_keys(self, keys):
    return self.base_query.filter(
      SongEntrie.song_key.in_(list(keys))
    ).all()

  def existing_keys(self):
    return self.session.query(SongEntrie.song_key).all()

  def get_metadata(self):
    return {
      "danceability": self.danceability,
      "energy": self.energy,
      "key": self.key,
      "loudness": self.loudness,
      "mode": self.mode,
      "speechiness": self.speechiness,
      "acousticness": self.acousticness,
      "instrumentalness": self.instrumentalness,
      "liveness": self.liveness,
      "valence": self.valence,
      "tempo": self.tempo,
      "duration_ms": self.duration_ms,
      "time_signature": self.time_signature
    }
