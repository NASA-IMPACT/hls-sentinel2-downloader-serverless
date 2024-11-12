import boto3
import requests

from app.subscription_endpoint import EndpointConfig


if __name__ == "__main__":
    # example notification data
    s1_notification_data = {
        "@odata.context": "$metadata#Notification/$entity",
        "SubscriptionEvent": "created",
        "ProductId": "test-notification-0dd083d4-360e-4693-9791-1215da02c3a3",
        "ProductName": "test-notification-S1A_S3_GRDH_1SDV_20230801T152912_20230801T152931_049683_05F970_0441_COG.SAFE",
        "SubscriptionId": "05d02462-6685-46e6-a1a4-bd1749330e43",
        "NotificationDate": "2024-04-03T10:08:38.507Z",
        "Value": {
            "@odata.context": "$metadata#Products(Attributes())(Assets())",
            "@odata.mediaContentType": "application/octet-stream",
            "Id": "test-notification-0dd083d4-360e-4693-9791-1215da02c3a3",
            "Name": "test-notification-S1A_S3_GRDH_1SDV_20230801T152912_20230801T152931_049683_05F970_0441_COG.SAFE",
            "ContentType": "application/octet-stream",
            "ContentLength": 161991897,
            "OriginDate": "2023-08-01T16:54:32.762Z",
            "Checksum": [{}],
            "ContentDate": {
                "Start": "2023-08-01T15:29:12.689Z",
                "End": "2023-08-01T15:29:31.839Z",
            },
            "Footprint": "geography'SRID=4326;POLYGON ((43.033947 -12.177707, 43.756836 -12.014938, 43.49361 -10.859819, 42.773415 -11.021481, 43.033947 -12.177707))'",
            "GeoFootprint": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [43.033947, -12.177707],
                        [43.756836, -12.014938],
                        [43.49361, -10.859819],
                        [42.773415, -11.021481],
                        [43.033947, -12.177707],
                    ]
                ],
            },
            "Attributes": [
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "origin",
                    "Value": "CLOUDFERRO",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.IntegerAttribute",
                    "Name": "datatakeID",
                    "Value": 391536,
                    "ValueType": "Integer",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "timeliness",
                    "Value": "Fast-24h",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.IntegerAttribute",
                    "Name": "cycleNumber",
                    "Value": 298,
                    "ValueType": "Integer",
                },
                {
                    "@odata.type": "#OData.CSC.IntegerAttribute",
                    "Name": "orbitNumber",
                    "Value": 49683,
                    "ValueType": "Integer",
                },
                {
                    "@odata.type": "#OData.CSC.IntegerAttribute",
                    "Name": "sliceNumber",
                    "Value": 1,
                    "ValueType": "Integer",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "productClass",
                    "Value": "S",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "processorName",
                    "Value": "Sentinel-1 IPF",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "orbitDirection",
                    "Value": "ASCENDING",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.DateTimeOffsetAttribute",
                    "Name": "processingDate",
                    "Value": "2023-08-01T16:43:32.478663+00:00",
                    "ValueType": "DateTimeOffset",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "operationalMode",
                    "Value": "SM",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "processingLevel",
                    "Value": "LEVEL1",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "swathIdentifier",
                    "Value": "S3",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "processingCenter",
                    "Value": "Production Service-SERCO",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "processorVersion",
                    "Value": "003.61",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "platformShortName",
                    "Value": "SENTINEL-1",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "instrumentShortName",
                    "Value": "SAR",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.IntegerAttribute",
                    "Name": "relativeOrbitNumber",
                    "Value": 86,
                    "ValueType": "Integer",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "polarisationChannels",
                    "Value": "VV&VH",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "platformSerialIdentifier",
                    "Value": "A",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.DoubleAttribute",
                    "Name": "startTimeFromAscendingNode",
                    "Value": 5712238,
                    "ValueType": "Double",
                },
                {
                    "@odata.type": "#OData.CSC.DoubleAttribute",
                    "Name": "completionTimeFromAscendingNode",
                    "Value": 5731388,
                    "ValueType": "Double",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "productType",
                    "Value": "S3_GRDH_1S-COG",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.DateTimeOffsetAttribute",
                    "Name": "beginningDateTime",
                    "Value": "2023-08-01T15:29:12.689Z",
                    "ValueType": "DateTimeOffset",
                },
                {
                    "@odata.type": "#OData.CSC.DateTimeOffsetAttribute",
                    "Name": "endingDateTime",
                    "Value": "2023-08-01T15:29:31.839Z",
                    "ValueType": "DateTimeOffset",
                },
            ],
            "ModificationDate": "2023-08-04T06:44:27.481Z",
            "PublicationDate": "2023-08-04T06:44:27.481Z",
            "Online": True,
            "EvictionDate": "",
            "S3Path": "/eodata/Sentinel-1/SAR/S3_GRDH_1S-COG/2023/08/01/S1A_S3_GRDH_1SDV_20230801T152912_20230801T152931_049683_05F970_0441_COG.SAFE",
            "Assets": [],
        },
    }

    s2_notification_data = {
        "@odata.context": "$metadata#Notification/$entity",
        "SubscriptionEvent": "created",
        "ProductId": "f867a59a-9336-46d0-93ae-e55bf29403f8",
        "ProductName": "S2A_MSIL1C_20240912T112541_N0511_R137_T28PHQ_20240912T133420.SAFE",
        "SubscriptionId": "880c4e1a-cfc7-4956-bc4c-4434069a0aa7",
        "NotificationDate": "2024-09-12T14:52:52.000Z",
        "AckId": "MTcyNjE1Mjc3MjMyMi0wOjg4MGM0ZTFhLWNmYzctNDk1Ni1iYzRjLTQ0MzQwNjlhMGFhNw==",
        "value": {
            "@odata.context": "$metadata#Products(Attributes())(Assets())(Locations())/$entity",
            "@odata.mediaContentType": "application/octet-stream",
            "Id": "f867a59a-9336-46d0-93ae-e55bf29403f8",
            "Name": "S2A_MSIL1C_20240912T112541_N0511_R137_T28PHQ_20240912T133420.SAFE",
            "ContentType": "application/octet-stream",
            "ContentLength": 132384463,
            "OriginDate": "2024-09-12T14:39:38.000Z",
            "PublicationDate": "2024-09-12T14:52:06.118Z",
            "ModificationDate": "2024-09-12T14:52:51.828Z",
            "Online": True,
            "EvictionDate": "9999-12-31T23:59:59.999Z",
            "S3Path": "/eodata/Sentinel-2/MSI/L1C/2024/09/12/S2A_MSIL1C_20240912T112541_N0511_R137_T28PHQ_20240912T133420.SAFE",
            "Checksum": [
                {
                    "Value": "700a3f2014ab3670408b94e4310924dd",
                    "Algorithm": "MD5",
                    "ChecksumDate": "2024-09-12T14:52:51.426545Z",
                },
                {
                    "Value": "fd35ef962ec7a09b190dc4049fe98906cc89f16e1a4322b622a49fc62e3f6e43",
                    "Algorithm": "BLAKE3",
                    "ChecksumDate": "2024-09-12T14:52:51.657328Z",
                },
            ],
            "ContentDate": {
                "Start": "2024-09-12T11:25:41.024Z",
                "End": "2024-09-12T11:25:41.024Z",
            },
            "Footprint": "geography'SRID=4326;POLYGON ((-11.974999865007275 9.033981046236953, -12.271487717962122 9.03659015317746, -12.27853242584367 8.044545576909323, -12.199694512664326 8.043927404167855, -12.18648545337299 8.102176718496475, -12.15282241132278 8.250773715771231, -12.119125071984612 8.399275118590491, -12.085423254585894 8.547673138011769, -12.051758447762111 8.695945283833096, -12.018066565499025 8.844240589995735, -11.984403225613384 8.992620036679964, -11.974999865007275 9.033981046236953))'",
            "GeoFootprint": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-11.974999865007275, 9.033981046236953],
                        [-12.271487717962122, 9.03659015317746],
                        [-12.27853242584367, 8.044545576909323],
                        [-12.199694512664326, 8.043927404167855],
                        [-12.18648545337299, 8.102176718496475],
                        [-12.15282241132278, 8.250773715771231],
                        [-12.119125071984612, 8.399275118590491],
                        [-12.085423254585894, 8.547673138011769],
                        [-12.051758447762111, 8.695945283833096],
                        [-12.018066565499025, 8.844240589995735],
                        [-11.984403225613384, 8.992620036679964],
                        [-11.974999865007275, 9.033981046236953],
                    ]
                ],
            },
            "Attributes": [
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "origin",
                    "Value": "ESA",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "tileId",
                    "Value": "28PHQ",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.DoubleAttribute",
                    "Name": "cloudCover",
                    "Value": 97.680833965944,
                    "ValueType": "Double",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "datastripId",
                    "Value": "S2A_OPER_MSI_L1C_DS_2APS_20240912T133420_S20240912T112539_N05.11",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.IntegerAttribute",
                    "Name": "orbitNumber",
                    "Value": 48182,
                    "ValueType": "Integer",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "sourceProduct",
                    "Value": "S2A_OPER_MSI_L1C_TL_2APS_20240912T133420_A048182_T28PHQ_N05.11,S2A_OPER_MSI_L1C_DS_2APS_20240912T133420_S20240912T112539_N05.11,S2A_OPER_MSI_L1C_TC_2APS_20240912T133420_A048182_T28PHQ_N05.11.jp2",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.DateTimeOffsetAttribute",
                    "Name": "processingDate",
                    "Value": "2024-09-12T13:34:20+00:00",
                    "ValueType": "DateTimeOffset",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "productGroupId",
                    "Value": "GS2A_20240912T112541_048182_N05.11",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "operationalMode",
                    "Value": "INS-NOBS",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "processingLevel",
                    "Value": "S2MSI1C",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "processorVersion",
                    "Value": "05.11",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "granuleIdentifier",
                    "Value": "S2A_OPER_MSI_L1C_TL_2APS_20240912T133420_A048182_T28PHQ_N05.11",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "platformShortName",
                    "Value": "SENTINEL-2",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "instrumentShortName",
                    "Value": "MSI",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.IntegerAttribute",
                    "Name": "relativeOrbitNumber",
                    "Value": 137,
                    "ValueType": "Integer",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "sourceProductOriginDate",
                    "Value": "2024-09-12T14:39:37Z,2024-09-12T14:39:33Z,2024-09-12T14:39:38Z",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "platformSerialIdentifier",
                    "Value": "A",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.StringAttribute",
                    "Name": "productType",
                    "Value": "S2MSI1C",
                    "ValueType": "String",
                },
                {
                    "@odata.type": "#OData.CSC.DateTimeOffsetAttribute",
                    "Name": "beginningDateTime",
                    "Value": "2024-09-12T11:25:41.024Z",
                    "ValueType": "DateTimeOffset",
                },
                {
                    "@odata.type": "#OData.CSC.DateTimeOffsetAttribute",
                    "Name": "endingDateTime",
                    "Value": "2024-09-12T11:25:41.024Z",
                    "ValueType": "DateTimeOffset",
                },
            ],
            "Assets": [
                {
                    "Type": "QUICKLOOK",
                    "Id": "97cd3cae-9999-4871-9767-9a44305415e6",
                    "DownloadLink": "https://catalogue.dataspace.copernicus.eu/odata/v1/Assets(97cd3cae-9999-4871-9767-9a44305415e6)/$value",
                    "S3Path": "/eodata/Sentinel-2/MSI/L1C/2024/09/12/S2A_MSIL1C_20240912T112541_N0511_R137_T28PHQ_20240912T133420.SAFE",
                }
            ],
            "Locations": [
                {
                    "FormatType": "Extracted",
                    "DownloadLink": "https://catalogue.dataspace.copernicus.eu/odata/v1/Products(f867a59a-9336-46d0-93ae-e55bf29403f8)/$value",
                    "ContentLength": 132384463,
                    "Checksum": [
                        {
                            "Value": "700a3f2014ab3670408b94e4310924dd",
                            "Algorithm": "MD5",
                            "ChecksumDate": "2024-09-12T14:52:51.426545Z",
                        },
                        {
                            "Value": "fd35ef962ec7a09b190dc4049fe98906cc89f16e1a4322b622a49fc62e3f6e43",
                            "Algorithm": "BLAKE3",
                            "ChecksumDate": "2024-09-12T14:52:51.657328Z",
                        },
                    ],
                    "S3Path": "/eodata/Sentinel-2/MSI/L1C/2024/09/12/S2A_MSIL1C_20240912T112541_N0511_R137_T28PHQ_20240912T133420.SAFE",
                }
            ],
        },
    }

    config = EndpointConfig.load_from_secrets_manager("event-subs")

    # send request to endpoint with Basic Authorization
    response = requests.post(
        # url="http://localhost:8000/events",
        url=config.get_endpoint_url(boto3.client("ssm")),
        json=s2_notification_data,
        auth=(
            config.notification_username,
            config.notification_password,
        ),
        headers={"Content-Type": "application/json"},
        verify=True,
    )
    response.raise_for_status()
    print(response)
