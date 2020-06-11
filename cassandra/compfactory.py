"""Utilities for creating cassandra components

Functions:
create_component: Create a component by name.
"""

import cassandra.components as comp

_available_components = {
    'Global': comp.GlobalParamsComponent,
    'GcamComponent': comp.GcamComponent,
    'FldgenComponent': comp.FldgenComponent,
    'HectorStubComponent': comp.HectorStubComponent,
    'TgavStubComponent': comp.TgavStubComponent,
    'TethysComponent': comp.TethysComponent,
    'XanthosComponent': comp.XanthosComponent,
    'DummyComponent': comp.DummyComponent
}


def create_component(compname, cap_tbl):
    """Create a component by name

    :param compname: Name of the component class to create. The first '.'
                     character (if any) and everything after it are ignored, so
                     you can disambiguate multiple copies of a component by
                     adding '.<unique-id>' to the end of the component name.
    :param cap_tbl: Capability table to use to initialize the component.
    :return: Newly created component.
    """

    # ignore everything following a '.' in the component name
    csplt = compname.split('.')
    compname = csplt[0].strip()

    if not compname in _available_components:
        raise RuntimeError(f'Unknown component type {compname}')

    return _available_components[compname](cap_tbl)

def add_new_component(compname, classobj):
    """Add a new type of component to the list of available components.

    :param compname: Name by which the new component will be known in 
                     configuration files.
    :param classobj: The class object for the class implementing the 
                     component.  The class MUST be a subclass of ComponentBase.

    Calling this function will add the requested component to the available
    components table, allowing it to be parsed from config files and instantiated
    by the system.  The class passed in as class object must be a subclass of 
    ComponentBase.  The class inheritance isn't checked, so passing some random class
    might appear to work, or it might work in actuality, but nothing is guaranteed.

    """

    global _available_components
    
    _available_components[compname] = classobj
    
