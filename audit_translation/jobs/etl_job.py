import awswrangler as wr
import pandas as pd
from datetime import date
from audit_translation.config import Config
from audit_translation.config.constants import CATEGORY_EXTRACT_FIELDS, \
    ASSOCIATION_ENTITIES, TRANSLATE_COLS, VALUE_COLS, COLUMN_COLS, TABLE_COLS, \
    PARTITION_COLS, PATH, INPUT_PATH

config = Config()


def apply_func(raw_row, mapping):
    import json
    raw_row['category'] = json.loads(raw_row['category'])
    row = {
        'owner': raw_row['owner_id'],
        'year': raw_row['timestamp'].year,
        'month': raw_row['timestamp'].month,
        'day': raw_row['timestamp'].day,
        'timestamp': raw_row['timestamp'],
        'user_id': raw_row['user_id'],
        **{k: raw_row['category'].get(k, '') for k in CATEGORY_EXTRACT_FIELDS},
        'entitySource2': '',
        'dataOperation2': ''
    }
    table = row['entitySource']

    if row['entitySource'] in ASSOCIATION_ENTITIES.keys():
        entities = ASSOCIATION_ENTITIES[row['entitySource']]
        row['entitySource'], row['entitySource2'] = entities
        row['dataOperation2'], row['dataOperation'] = row['dataOperation'], ''

    col = row['columnName']

    for attr in TRANSLATE_COLS:
        if not row[attr]:
            continue

        if attr in TABLE_COLS:
            index = row[attr], '', ''

        if table == 'ps_interoperability_int':
            key = raw_row['category'].get('entityId2')
            if attr in COLUMN_COLS:
                if col in ('value', 'isPassword'):
                    field = translate(mapping, (table, key, ''), row[attr])
                    aspect = translate(mapping, (table, col, ''), row[attr])
                    index = f'{field} - {aspect}'
                else:
                    index = table, col, ''
            elif attr in VALUE_COLS:
                if col == 'value':
                    index = table, key, row[attr]
                else:
                    index = table, col, row[attr]
        else:
            if attr in COLUMN_COLS:
                index = table, col, ''
            elif attr in VALUE_COLS:
                index = table, col, row[attr]

        row[attr] = translate(mapping, index, row[attr]) if \
            isinstance(index, tuple) else index

    row = dict(sorted(row.items(), key=lambda x: x[0]))
    return row


def translate(mapping, index, default):
    if index in mapping.index:
        return mapping.loc[index]['translation']
    else:
        if (index[:2] in mapping.index and len(mapping.loc[index[:2]]) > 1) \
                or index[2] == '':
            print('Translation does not exist for index: ' + '/'.join(index))
        return default


def extract():
    year, month, day = [str(int(x)) for x in date.today().isoformat().split('-')]
    return wr.s3.read_parquet(
        f's3://{getattr(config, INPUT_PATH)}/data/audit/', dataset=True,
        partition_filter=lambda x: x['year'] == year and x['month'] == month and x['day'] == day)


def extract_mapping():
    con = wr.redshift.connect('CONFIGURAR UNA AWS GLUE CONNECTION HACIA REDSHIFT Y REEMPLAZAR ESTE STRING CON EL NOMBRE DE LA CONEXIÓN. https://docs.aws.amazon.com/glue/latest/dg/console-connections.html?icmpid=docs_glue_console')
    with con.cursor() as cursor:
        cursor.execute(
            'select column_name_coh as column, table_name_coh as table, '
            'value_coh as value, translation_coh as translation '
            'from audit_test.config_homolagacion_coh')
        mapping = [dict(row) for row in cursor.fetchall()]
    con.close()
    return pd.DataFrame(mapping)


def transform(records, mapping):
    mapping.set_index(['table', 'column', 'value'], inplace=True)
    return records.apply(apply_func, 1, result_type='expand', mapping=mapping)


def load(df):
    wr.s3.to_parquet(df, f's3://{getattr(config, PATH)}', dataset=True,
                     partition_cols=PARTITION_COLS)


def main():
    records = extract()
    mapping = extract_mapping()
    df = transform(records, mapping)
    load(df)


if __name__ == '__main__':
    main()
