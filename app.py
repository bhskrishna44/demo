import json
import os
import pandas
import glob
def get_columns(ds):
    with open('data/retail_db/schemas.json') as f:
        schemas = json.load(f)
    try:
        schema = schemas.get(ds)
        if not schema:
            raise KeyError
        else:
            col = sorted(schema, key= lambda s: s['column_position'])
            columns = [i['column_name'] for i in col]
            return columns
    except KeyError:
        print(f'Schema does no exists for {ds}')
        return

import uuid
def main():
    os.makedirs('data/retail_demo')
    for i in glob.glob('data/retail_db/*'):
        if os.path.isdir(i):
            ds = os.path.split(i)[1]
            for j in glob.glob(f'{i}/*'):
                data = pandas.read_csv(f'{j}', names=get_columns(ds))
                os.makedirs(f'data/retail_demo/{ds}')
                data.to_json(f'data/retail_demo/{ds}/part-{uuid.uuid1}',
                            lines=True,
                            orient='records'            
                )
                print(f'Number of records in {os.path.split(j)[1]} from {ds} are {data.shape[0]}')

        if __name__=="__main__":
            main()