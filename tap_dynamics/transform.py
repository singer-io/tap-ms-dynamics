import json

def flatten_entity_attributes(attributes):
    flat_attributes = {}
    # dict of attributes/fields for an entity in {'LogicalNameOfAttribute': 'DynamicsDataType', ...}
    for attr in attributes:
        logical_name = attr.get('LogicalName')
        dynamics_type = attr.get('AttributeTypeName', {}).get('Value')
        is_readable = attr.get('IsValidForRead')
        flat_attributes.update({logical_name: {'type': dynamics_type, 'is_readable': is_readable}})

    return flat_attributes
