from .wetlands import Wetlands
from .cadastral import Cadastral
from .water_projects import WaterProjects
from .agricultural_fields import AgriculturalFields

def get_source_handler(source_id: str, config: dict):
    """Get appropriate source handler based on source ID"""
    handlers = {
        'water_projects': WaterProjects,
        'wetlands': Wetlands,
        'cadastral': Cadastral,
        'agricultural_fields': AgriculturalFields
    }
    
    handler_class = handlers.get(source_id)
    if handler_class:
        return handler_class(config)
    return None
