import math

def bounding_box(coord, max_distance_km):
    """
    Calculate a bounding box around a coordinate such that all points
    within the box are within `max_distance_km` from the coordinate.

    Parameters:
        coord: tuple (lat, lon) in decimal degrees
        max_distance_km: maximum distance in kilometers

    Returns:
        (min_lat, max_lat, min_lon, max_lon)
    """
    lat, lon = coord
    R = 6371.0  # Earth radius in km

    # Latitude bounds (1 degree ≈ 111 km)
    delta_lat = (max_distance_km / R) * (180 / math.pi)
    min_lat = lat - delta_lat
    max_lat = lat + delta_lat

    # Longitude bounds depend on latitude
    delta_lon = (max_distance_km / R) * (180 / math.pi) / math.cos(math.radians(lat))
    min_lon = lon - delta_lon
    max_lon = lon + delta_lon

    return (min_lat, max_lat, min_lon, max_lon)