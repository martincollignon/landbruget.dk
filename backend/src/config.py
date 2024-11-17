SOURCES = {
    "agricultural_fields": {
        "name": "Danish Agricultural Fields",
        "type": "wfs",
        "description": "Weekly updated agricultural field data",
        "url": "https://geodata.fvm.dk/geoserver/wfs",
        "layer": "Marker:Marker_seneste",
        "frequency": "weekly",
        "enabled": True
    },
    "wetlands": {
        "name": "Danish Wetlands Map",
        "type": "shapefile",
        "description": "Wetland areas from Danish EPA",
        "filename": "kulstof2022",
        "frequency": "static",
        "enabled": True
    }
}
