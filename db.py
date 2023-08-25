from typing import Generator, Union
from src.song import Song

from .tables.song_entrie import SongEntrie
from .tables.artist_table import Artist
from .tables.album_table import Album

from .db_config import session

from ..groupper import Groupper

TABLES_UNION = Union[SongEntrie, Album, Artist]

SONGS_TABLE = SongEntrie(session=session)
ARTIST_TABLE = Artist(session=session)
ALBUM_TABLE = Album(session=session)

SONGS_TABLE.metadata.create_all()

def existing_songs_keys():
  return SONGS_TABLE.existing_keys()

def insert_song(song: Song):
  insert_songs([song])

def populate_artist_and_album(func):
  def populate(songs: list[Song]):
    # This is ugly please don't look
    by_artist = Groupper(songs).by_artist()
    for artist, artist_songs in by_artist.items():
      by_album = Groupper(artist_songs).by_album()
      artist: Artist = find_or_create_artist(artist)
      for album, album_songs in by_album.items():
        album: Album = find_or_create_album(album, artist)
        album.artist_id = artist.id
        for song in album_songs:
          song.artist = artist
          song.album = album

    func(songs)

  return populate

def get_all_songs() -> list[SongEntrie]:
  return SONGS_TABLE.get_all()

@populate_artist_and_album
def insert_songs(songs: list[Song]):
  SONGS_TABLE.insert_rows(songs)

def find_or_create_album(album: str, artist: str) -> Album:
  return find_or_create(ALBUM_TABLE, name=album, artist=artist)

def find_or_create_artist(artist: str) -> Artist:
  return find_or_create(ARTIST_TABLE, name=artist)

# def find_or_create_all(table: TABLES_UNION) -> TABLES_UNION:
#   return table.by_keys

def find_or_create(table: TABLES_UNION, **kwargs) -> TABLES_UNION:
  return table.find_or_create_by_name(**kwargs)

def songs_by_ids(songs: list[Song]):
  return by_ids(
    SONGS_TABLE,
    songs
  )

def by_songs_keys(keys: list[str]) -> list[SongEntrie]:
  return SONGS_TABLE.by_keys(keys)

def by_ids(table: TABLES_UNION, ids: list[str]):
  return table.by_ids(ids)

def insert_stats():
  ARTIST_TABLE.calculate_stats()
  ALBUM_TABLE.calculate_stats()

