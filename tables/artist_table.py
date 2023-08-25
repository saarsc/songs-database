from sqlalchemy.orm import relationship
from .stats_based_table import StatsBasedTable
from ..db_config import Base

class Artist(StatsBasedTable, Base):
  __tablename__ = "artist"

  albums = relationship("Album", back_populates="artist")
  songs = relationship("SongEntrie", back_populates="artist")

  def get_metadata(self) -> dict:
    metadata = super().get_metadata()
    metadata["albums_ids"] = [album.id for album in self.albums]
    return metadata
