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
        "type": "wfs",
        "description": "Wetland areas from Danish EPA",
        "url": "https://wfs2-miljoegis.mim.dk/natur/wfs",
        "layer": "natur:kulstof2022",
        "frequency": "static",
        "enabled": True
    },
        "cadastral": {
        "name": "Danish Cadastral Properties",
        "type": "wfs",
        "description": "Current real estate property boundaries",
        "url": "https://wfs.datafordeler.dk/MATRIKLEN2/MatGaeldendeOgForeloebigWFS/1.0.0/WFS",
        "frequency": "weekly",
        "enabled": True
    },
    "water_projects": {
        "name": "Danish Water Projects",
        "type": "wfs",
        "description": "Water projects from various Danish programs",
        "url": "https://geodata.fvm.dk/geoserver/wfs",
        "frequency": "weekly",
        "enabled": True
    }
}
