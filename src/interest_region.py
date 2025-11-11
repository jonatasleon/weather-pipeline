from typing import TypedDict


class Region(TypedDict):
    name: str
    latitude: float
    longitude: float


REGIONS: list[Region] = [
    {"name": "São Paulo", "latitude": -23.55, "longitude": -46.63},
    {"name": "Brasília", "latitude": -15.78, "longitude": -47.93},
    {"name": "Itajubá", "latitude": -15.47, "longitude": -47.27},
    {"name": "São José dos Campos", "latitude": -23.18, "longitude": -45.88},
    {"name": "Guaratinguetá", "latitude": -22.82, "longitude": -45.20},
    {"name": "Cruzeiro", "latitude": -17.47, "longitude": -43.03},
]
