from typing import List
import datetime as dt
from sqlalchemy import ForeignKey, Table, Column, Integer, String, Time
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass

# -----------------------
# Routes data
# -----------------------

class Routes(Base):
    __tablename__ = "routes"

    route_id=Column(Integer, primary_key=True)
    name=Column(String, nullable=True)
    lat: Mapped[float]
    lon: Mapped[float]
    snow_ice_mixed=Column(Integer, nullable=True)
    mountain_climbing=Column(Integer, nullable=True)
    ice_climbing=Column(Integer, nullable=True)
    elevation_min=Column(Integer, nullable=True)
    elevation_max=Column(Integer, nullable=True)
    difficulties_height=Column(Integer, nullable=True)
    height_diff_difficulties=Column(Integer, nullable=True)
    glacier=Column(String, nullable=True) 
    global_rating=Column(String, nullable=True) 
    ice_rating=Column(String, nullable=True) 
    mixed_rating=Column(String, nullable=True) 
    rock_free_rating=Column(String, nullable=True) 
    last_updated=Column(String, nullable=True) 

    orientations: Mapped[List["Orientations"]] = relationship(
        secondary="orientation_mapping", #For a many-to-many relationship, specifies the intermediary table, and is typically an instance of Table
    )

    countries: Mapped[List["Countries"]] = relationship(
        secondary="countries_mapping", 
    )

    outings: Mapped[List["Outings"]] = relationship(
        secondary="outings_mapping", back_populates='routes'
    )

    stations: Mapped[List["WeatherStation"]] = relationship(
            secondary="route_stations_mapping", back_populates='routes'
        )
    
    def __repr__(self):
         return f"Route(id={self.route_id}, link=https://www.camptocamp.org/routes/{self.route_id}, count_outings={len(self.outings)})"


# From https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html#many-to-many:
# note for a Core table, we use the sqlalchemy.Column construct,
# not sqlalchemy.orm.mapped_column
orientation_mapping = Table(
    "orientation_mapping",
    Base.metadata,
    Column("route_id", ForeignKey("routes.route_id"), primary_key=True),
    Column("orientation_id", ForeignKey("orientations.orientation_id"), primary_key=True),
)

countries_mapping = Table(
    "countries_mapping",
    Base.metadata,
    Column("route_id", ForeignKey("routes.route_id"), primary_key=True),
    Column("country_id", ForeignKey("countries.country_id"), primary_key=True),
)

outings_mapping = Table(
    "outings_mapping",
    Base.metadata,
    Column("route_id", ForeignKey("routes.route_id"), primary_key=True),
    Column("outing_id", ForeignKey("outings.outing_id"), primary_key=True),
)

class Countries(Base):
    __tablename__ = "countries"

    country_id=Column(Integer, primary_key=True)
    countryName: Mapped[str] = mapped_column(nullable=True)

    def __repr__(self):
         return f"Country(id={self.country_id}, name={self.countryName})"

class Orientations(Base):
    __tablename__ = "orientations"

    orientation_id=Column(Integer, primary_key=True)
    orientation: Mapped[str] = mapped_column(unique=True)

    def __repr__(self):
         return f"Orientation(id={self.orientation_id}, orientation={self.orientation})"
    
class Outings(Base):
    __tablename__ = "outings"

    outing_id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[str] 
    conditions=Column(String, nullable=True) 
    last_updated=Column(String, nullable=True) 
    
    routes: Mapped[List["Routes"]] = relationship(
        secondary="outings_mapping", back_populates='outings'
    )

    def __repr__(self):
         return f"Outing(id={self.outing_id}, link=https://www.camptocamp.org/outings/{self.outing_id})"


# -----------------------
# Weather data
# -----------------------

class WeatherStation(Base):

    __tablename__ = "weather_stations"

    station_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    date_start: Mapped[dt.datetime]
    date_end: Mapped[dt.datetime]
    altitude: Mapped[int] = mapped_column(nullable=False)
    lat: Mapped[float] = mapped_column(nullable=False)
    lon: Mapped[float] = mapped_column(nullable=False)
    last_updated: Mapped[dt.datetime]
    of_interest: Mapped[bool] #Used to flag stations that are worth scraping 

    parameters: Mapped[List["StationsParameters"]] = relationship(
        secondary="stations_parameters_mapping", back_populates='stations'
    )
    routes: Mapped[List["Routes"]] = relationship(
        secondary="route_stations_mapping", back_populates='stations'
    )

    def __repr__(self):
        return f"WeatherStation(id={self.station_id}, name={self.name}, altitude={self.altitude})"


class StationsParameters(Base):

    __tablename__ = "stations_parameters"

    parameter_id: Mapped[int] = mapped_column(primary_key=True)
    parameter_name: Mapped[str] = mapped_column(nullable=False)
    last_updated: Mapped[dt.datetime]

    stations: Mapped[List["WeatherStation"]] = relationship(
            secondary="stations_parameters_mapping", back_populates='parameters'
        )

    def __repr__(self):
        return f"StationParameter(id={self.parameter_id}, name={self.parameter_name})"

stations_parameters_mapping = Table(
    "stations_parameters_mapping",
    Base.metadata,
    Column("station_id", ForeignKey("weather_stations.station_id"), primary_key=True),
    Column("parameter_id", ForeignKey("stations_parameters.parameter_id"), primary_key=True),
)

# -----------------------
# Link table
# -----------------------

route_stations_mapping = Table(
    "route_stations_mapping",
    Base.metadata,
    Column("station_id", ForeignKey("weather_stations.station_id"), primary_key=True),
    Column("route_id", ForeignKey("routes.route_id"), primary_key=True),
)

