DATA_SOURCE = '/dev/job_audit_translation/data_source'
PATH = '/dev/job_audit_translation/bucket_path'
INPUT_PATH = '/dev/job-auditCollector/bucket_path'

ASSOCIATION_ENTITIES = {
    'ms_relation_device_rde': ('ms_service_point_spo', 'ms_device_dev'),
    'ms_agent_metering_agm': ('ms_agent_agt', 'ms_service_point_spo'),
    'ms_service_point_variable_spv': ('ms_service_point_spo',
                                      'ms_bridge_variables')
}

TABLE_COLS = 'entitySource', 'entitySource2', 'dataOperation', 'dataOperation2'
COLUMN_COLS = 'columnName',
VALUE_COLS = 'oldValue', 'newValue'
TRANSLATE_COLS = TABLE_COLS + COLUMN_COLS + VALUE_COLS


CATEGORY_EXTRACT_FIELDS = ('entityId1', 'entityId2', 'columnName', 'oldValue',
                           'newValue', 'dataOperation', 'entitySource',
                           'auditTrackId')

PARTITION_COLS = ['owner', 'year', 'month', 'day']
