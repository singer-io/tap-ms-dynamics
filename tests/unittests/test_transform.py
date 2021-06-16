from tap_dynamics.transform import (flatten_entity_attributes,
                                    transform_metadata_xml)

def test_flatten_entity_attributes():
    test_cases = [
        {
            'case': [
                {'LogicalName': 'accountid', 'PropertyType': 'Edm.Guid'},
                {'LogicalName': 'accountnumber', 'PropertyType': 'Edm.String'},
                {'LogicalName': 'createdon', 'PropertyType': 'Edm.DateTimeOffset'},
        ],
            'expected': {
                'accountid': {'type': 'Edm.Guid'},
                'accountnumber': {'type': 'Edm.String'},
                'createdon': {'type': 'Edm.DateTimeOffset'}},
        },
        {
            'case': [
                {'LogicalName': 'firstname', 'PropertyType': 'Edm.String'},
                {'LogicalName': 'donotphone', 'PropertyType': 'Edm.Boolean'},
                {'LogicalName': 'anniversary', 'PropertyType': 'Edm.Date'},
            ],
            'expected': {
                'firstname': {'type': 'Edm.String'},
                'donotphone': {'type': 'Edm.Boolean'},
                'anniversary': {'type': 'Edm.Date'},
            }
        }
    ]

    for test_case in test_cases:
        result = flatten_entity_attributes(test_case['case'])

        assert test_case['expected'] == result

def test_transform_metadata_xml():
    xml_string = '''<?xml version="1.0" encoding="utf-8"?>
    <edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:Reference Uri="http://vocabularies.odata.org/OData.Community.Keys.V1.xml">
            <edmx:Include Namespace="OData.Community.Keys.V1" Alias="Keys" />
            <edmx:IncludeAnnotations TermNamespace="OData.Community.Keys.V1" />
        </edmx:Reference>
        <edmx:Reference Uri="http://vocabularies.odata.org/OData.Community.Display.V1.xml">
            <edmx:Include Namespace="OData.Community.Display.V1" Alias="Display" />
            <edmx:IncludeAnnotations TermNamespace="OData.Community.Display.V1" />
        </edmx:Reference>
        <edmx:DataServices>
            <Schema Namespace="Microsoft.Dynamics.CRM" Alias="mscrm" xmlns="http://docs.oasis-open.org/odata/ns/edm">
                <EntityType Name="account" BaseType="mscrm.crmbaseentity">
                    <Key>
                        <PropertyRef Name="accountid" />
                    </Key>
                    <Property Name="accountid" Type="Edm.Guid" />
                    <Property Name="accountnumber" Type="Edm.String" />
                    <Property Name="createdon" Type="Edm.DateTimeOffset" />
                </EntityType>
                <EntityType Name="contact" BaseType="mscrm.crmbaseentity">
                    <Key>
                        <PropertyRef Name="contactid" />
                    </Key>
                    <Property Name="firstname" Type="Edm.String" />
                    <Property Name="donotphone" Type="Edm.Boolean" />
                    <Property Name="anniversary" Type="Edm.Date" />
                </EntityType>
            </Schema>
        </edmx:DataServices>
    </edmx:Edmx>
    '''
    expected = {
        'account': {
            'Key': 'accountid',
            'Properties': [
                {'LogicalName': 'accountid', 'PropertyType': 'Edm.Guid'},
                {'LogicalName': 'accountnumber', 'PropertyType': 'Edm.String'},
                {'LogicalName': 'createdon', 'PropertyType': 'Edm.DateTimeOffset'},
            ]
        },
        'contact': {
            'Key': 'contactid',
            'Properties': [
                {'LogicalName': 'firstname', 'PropertyType': 'Edm.String'},
                {'LogicalName': 'donotphone', 'PropertyType': 'Edm.Boolean'},
                {'LogicalName': 'anniversary', 'PropertyType': 'Edm.Date'},
            ]
        }
    }

    result = transform_metadata_xml(xml_string)

    assert expected == result
