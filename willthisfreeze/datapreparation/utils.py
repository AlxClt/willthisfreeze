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

def haversine_distance(coord1, coord2):
    """
    Calculate the great-circle distance between two points on Earth 
    given as (latitude, longitude) pairs using the Haversine formula.
    
    Parameters:
        coord1: tuple of (lat1, lon1) in decimal degrees
        coord2: tuple of (lat2, lon2) in decimal degrees

    Returns:
        Distance in kilometers (float)
    """
    # Radius of Earth in kilometers
    R = 6371.0

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert decimal degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance
