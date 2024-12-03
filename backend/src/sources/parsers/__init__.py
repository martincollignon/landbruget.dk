from .agricultural_fields import AgriculturalFields
from .wetlands import Wetlands
from .water_projects import WaterProjects
from .cadastral import Cadastral

def get_source_handler(source_id: str, config: dict):
    """Factory function to get the appropriate source handler"""
    handlers = {
        "agricultural_fields": AgriculturalFields,
        "wetlands": Wetlands,
        "water_projects": WaterProjects,
        "cadastral": Cadastral
    }
    
    handler_class = handlers.get(source_id)
    if handler_class:
        return handler_class(config)
    return None
