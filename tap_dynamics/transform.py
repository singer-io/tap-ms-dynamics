from xml.etree import ElementTree as ET

NS = {
    "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
    "edm": "http://docs.oasis-open.org/odata/ns/edm"
}

def flatten_entity_attributes(attributes:list) -> dict:
    flat_attributes = {}

    for attr in attributes:
        logical_name = attr.get('LogicalName')
        dynamics_type = attr.get('PropertyType')

        flat_attributes.update({logical_name: {'type': dynamics_type}})

    return flat_attributes

def transform_metadata_xml(xml:str) -> dict:
    tree = ET.fromstring(xml)

    data_service = tree.find("edmx:DataServices", NS)
    entities = data_service.find("edm:Schema", NS)

    entity_def = {}
    for elem in entities.findall("edm:EntityType", NS):
        # if an Entity doesn't have elements or a `Key` skip over it
        if len(elem) and elem.find("edm:Key", NS):
            entity_key = elem.find("edm:Key", NS).find("edm:PropertyRef", NS).get("Name")
            entity_name = elem.get("Name")

            props = []
            for prop in elem.findall("edm:Property", NS):
                prop_name = prop.get("Name")
                prop_type = prop.get("Type")
                props.append({"LogicalName": prop_name, "PropertyType": prop_type})

            entity_def.update({entity_name: {"Key": entity_key, "Properties": props}})

    return entity_def
