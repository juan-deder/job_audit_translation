import pandas as pd
from json import dumps
from unittest import TestCase
from datetime import datetime
from audit_translation.jobs.etl_job import transform

COMMON_FIELDS = {
    'timestamp': datetime(2020, 12, 12),
    'owner': 'PRIME',
    'year': 2020,
    'month': 12,
    'day': 12,
    'user_id': 'sameUser',
    'auditTrackId': f'2000-MS',
}


class AuditTranslationTestCase(TestCase):
    TRANSLATION = pd.DataFrame([
        {'table': 'UPDATE', 'column': '', 'value': '', 'translation': 'Actualizar'},

        {'table': 'ps_interoperability_int', 'column': '', 'value': '', 'translation': 'Sistema externo'},
        {'table': 'ps_interoperability_int', 'column': 'desc_int', 'value': '', 'translation': 'Descripción'},
        {'table': 'ps_interoperability_int', 'column': 'value', 'value': '', 'translation': 'Valor'},
        {'table': 'ps_interoperability_int', 'column': 'isPassword', 'value': '', 'translation': 'Es contraseña'},
        {'table': 'ps_interoperability_int', 'column': 'isPassword', 'value': 'true', 'translation': 'Sí'},
        {'table': 'ps_interoperability_int', 'column': 'isPassword', 'value': 'false', 'translation': 'No'},
        {'table': 'ps_interoperability_int', 'column': '461', 'value': '', 'translation': 'URL Servicio Web'},
        {'table': 'ps_interoperability_int', 'column': '733', 'value': '', 'translation': 'Estándar'},
        {'table': 'ps_interoperability_int', 'column': '733', 'value': '29', 'translation': 'Multispeak v5.0'},
        {'table': 'ps_interoperability_int', 'column': '733', 'value': '30', 'translation': 'AMRDef v6.0'},

        {'table': 'ms_service_point_spo', 'column': '', 'value': '', 'translation': 'Punto de servicio'},
        {'table': 'ms_device_dev', 'column': '', 'value': '', 'translation': 'Dispositivo'},

        {'table': 'ms_relation_device_rde', 'column': 'id_metering_type_dev', 'value': '', 'translation': 'Tipo de medida'},
        {'table': 'ms_relation_device_rde', 'column': 'id_metering_type_dev', 'value': '19', 'translation': 'Main'},
        {'table': 'ms_relation_device_rde', 'column': 'id_metering_type_dev', 'value': '20', 'translation': 'Backup'}
    ])

    def test_regular_audit_translation(self):
        records = self.craft_audit([{
            'entityIdName1': 'number_spo',
            'entityId1': f'SOC_V1_PAP_{i}',
            'entityIdName2': 'number_dev',
            'entityId2': f'DEV_V1_PAP_{i}',
            'columnName': 'id_metering_type_dev',
            'oldValue': '19',
            'newValue': '20',
            'entitySource': 'ms_relation_device_rde',
        } for i in range(10)])
        df = transform(records, self.TRANSLATION.copy())
        correct = pd.DataFrame([{
            **COMMON_FIELDS,
            'entityId1': f'SOC_V1_PAP_{i}',
            'entityId2': f'DEV_V1_PAP_{i}',
            'columnName': 'Tipo de medida',
            'oldValue': 'Main',
            'newValue': 'Backup',
            'dataOperation': '',
            'entitySource': 'Punto de servicio',
            'entitySource2': 'Dispositivo',
            'dataOperation2': 'Actualizar'} for i in range(10)
        ])
        self.assertTrue(df.equals(correct[sorted(correct.columns.to_list())]))

    def test_relation_translation(self):
        records = self.craft_audit({
            'entityIdName1': 'number_spo',
            'entityId1': 'SOC_V1_PAP_0',
            'entityIdName2': 'number_dev',
            'entityId2': 'DEV_V1_PAP_0',
            'columnName': 'id_metering_type_dev',
            'oldValue': '19',
            'newValue': '20',
            'entitySource': 'ms_relation_device_rde',
        })
        df = transform(records, self.TRANSLATION.copy())
        correct = pd.DataFrame([{
            **COMMON_FIELDS,
            'columnName': 'Tipo de medida',
            'dataOperation': '',
            'dataOperation2': 'Actualizar',
            'entityId1': 'SOC_V1_PAP_0',
            'entityId2': 'DEV_V1_PAP_0',
            'entitySource': 'Punto de servicio',
            'entitySource2': 'Dispositivo',
            'newValue': 'Backup',
            'oldValue': 'Main',
            'user_id': 'sameUser'
        }])
        self.assertTrue(df.equals(correct[sorted(correct.columns.to_list())]))

    def test_description_HES_audit(self):
        records = self.craft_audit({
            'entityIdName1': 'externalSystem-Connection',
            'entityId1': 'juan - CONEXION4',
            'entityIdName2': None,
            'entityId2': None,
            'columnName': 'desc_int',
            'oldValue': 'NEWDES---------------7',
            'newValue': 'NEWDES---------------8',
            'entitySource': 'ps_interoperability_int',
        })
        df = transform(records, self.TRANSLATION.copy())
        correct = pd.DataFrame([{
            **COMMON_FIELDS,
            'columnName': 'Descripción',
            'dataOperation': 'Actualizar',
            'dataOperation2': '',
            'entityId1': 'juan - CONEXION4',
            'entityId2': None,
            'entitySource': 'Sistema externo',
            'entitySource2': '',
            'newValue': 'NEWDES---------------8',
            'oldValue': 'NEWDES---------------7',
            'user_id': 'sameUser'
        }])
        self.assertTrue(df.equals(correct[sorted(correct.columns.to_list())]))

    def test_password_HES_audit(self):
        records = self.craft_audit({
            'entityIdName1': 'name_int - conexion_name_int',
            'entityId1': 'juan - CONEXION4',
            'entityIdName2': 'idKey',
            'entityId2': '461',
            'columnName': 'isPassword',
            'oldValue': 'true',
            'newValue': 'false',
            'entitySource': 'ps_interoperability_int',
        })
        df = transform(records, self.TRANSLATION.copy())
        correct = pd.DataFrame([{
            **COMMON_FIELDS,
            'columnName': 'URL Servicio Web - Es contraseña',
            'dataOperation': 'Actualizar',
            'dataOperation2': '',
            'entityId1': 'juan - CONEXION4',
            'entityId2': '461',
            'entitySource': 'Sistema externo',
            'entitySource2': '',
            'newValue': 'No',
            'oldValue': 'Sí',
            'user_id': 'sameUser'
        }])
        self.assertTrue(df.equals(correct[sorted(correct.columns.to_list())]))

    def test_plain_value_HES_audit(self):
        records = self.craft_audit({
            'columnName': 'value',
            'entityId1': 'juan - CONEXION4',
            'entityId2': '461',
            'entityIdName1': 'name_int - conexion_name_int',
            'entityIdName2': 'idKey',
            'entitySource': 'ps_interoperability_int',
            'newValue': 'q13',
            'oldValue': 'q12',
        })
        df = transform(records, self.TRANSLATION.copy())
        correct = pd.DataFrame([{
            **COMMON_FIELDS,
            'columnName': 'URL Servicio Web - Valor',
            'dataOperation': 'Actualizar',
            'dataOperation2': '',
            'entityId1': 'juan - CONEXION4',
            'entityId2': '461',
            'entitySource': 'Sistema externo',
            'entitySource2': '',
            'newValue': 'q13',
            'oldValue': 'q12',
        }])
        self.assertTrue(df.equals(correct[sorted(correct.columns.to_list())]))

    def test_domain_value_HES_audit(self):
        records = self.craft_audit({
            'columnName': 'value',
            'entityId1': 'juan - CONEXION4',
            'entityId2': '733',
            'entityIdName1': 'name_int - conexion_name_int',
            'entityIdName2': 'idKey',
            'entitySource': 'ps_interoperability_int',
            'newValue': '29',
            'oldValue': '30',
        })
        df = transform(records, self.TRANSLATION.copy())
        correct = pd.DataFrame([{
            **COMMON_FIELDS,
            'columnName': 'Estándar - Valor',
            'dataOperation': 'Actualizar',
            'dataOperation2': '',
            'entityId1': 'juan - CONEXION4',
            'entityId2': '733',
            'entitySource': 'Sistema externo',
            'entitySource2': '',
            'newValue': 'Multispeak v5.0',
            'oldValue': 'AMRDef v6.0',
        }])
        self.assertTrue(df.equals(correct[sorted(correct.columns.to_list())]))

    @classmethod
    def craft_audit(cls, category):
        return pd.DataFrame([{
            'transaction_id': 1,
            'message': 'Updated entity',
            'log_level': 'WARN',
            'owner_id': 'PRIME',
            'source_id': 'PlatformSettings',
            'log_type': 'PAP-LOG-auditory',
            'category': dumps({
                'sourceIp': f'1.1.1.1',
                'categoryName': 'Audit',
                'dataOperation': 'UPDATE',
                'auditTrackId': f'2000-MS',
                **c}),
            **COMMON_FIELDS
        } for c in ([category] if isinstance(category, dict) else category)])
