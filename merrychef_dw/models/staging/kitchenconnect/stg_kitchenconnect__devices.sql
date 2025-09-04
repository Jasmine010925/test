WITH all_devices AS (
    {% set source_tables = [
        'kc_pura_vida__Devices',
        'kc_tim_hortons__Devices', 
        'km_merrychef__Devices'
    ] %}
    {% for table in source_tables %}
    SELECT path, level, model, nodeid, status, street, brandid, country, pincode, deviceid, timezone, "modelType", productid, customerid, "imeiNumber", locationid, "assetNumber", "networkType", "customerName", "imeiSVNumber", "locationType", "locationName", "networkStatus", "simCardNumber", "locationTypeId", "networkProvider", "softwareVersion", "networkSignalLevel", _airbyte_extracted_at
    FROM {{ source('kitchenconnect', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

final AS (
    SELECT
        path,
        level,
        model,
        nodeid as node_id,
        status,
        street,
        brandid as brand_id,
        country,
        pincode,
        deviceid as device_id,
        timezone,
        "modelType" as model_type,
        productid as product_id,
        customerid as customer_id,
        "imeiNumber" as imei_number,
        locationid as location_id,
        "assetNumber" as asset_number,
        "networkType" as network_type,
        "customerName" as customer_name,
        "imeiSVNumber" as imei_sv_number,
        "locationType" as location_type,
        "locationName" as location_name,
        "networkStatus" as network_status,
        "simCardNumber" as sim_card_number,
        "locationTypeId" as location_type_id,
        "networkProvider" as network_provider,
        "softwareVersion" ->> 'firmwareVersionIot' as firmware_version_iot,
        "softwareVersion" ->> 'firmwareVersionQts' as firmware_version_qts,
        "softwareVersion" ->> 'firmwareVersionSrb' as firmware_version_srb,
        "networkSignalLevel" as network_signal_level,
        "_airbyte_extracted_at" as data_updated
    FROM all_devices
)

SELECT * FROM final
