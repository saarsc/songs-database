from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, ForeignKey
from .stats_based_table import StatsBasedTable
from ..db_config import Base

class Album(StatsBasedTable, Base):
  __tablename__ = "album"

  songs = relationship("SongEntrie", back_populates="album")
  artist_id = Column(Integer, ForeignKey("artist.id"))
  artist = relationship("Artist", back_populates="albums")

  def get_metadata(self) -> dict:
    metadata = super().get_metadata()
    metadata["songs_ids"] = [song.id for song in self.songs]
    return metadata
