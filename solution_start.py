import argparse
import os
import sys

import pandas as pd
import json
import time


def calculate_time(func):
    # added arguments inside the inner1,
    # if function takes any arguments,
    # can be added like this.
    def inner1(*args, **kwargs):
        # storing time before function execution
        begin = time.time()

        func(*args, **kwargs)

        # storing time after function execution
        end = time.time()
        print("Total time taken in : ", func.__name__, round(end - begin, 4), ' Secs')

    return inner1

'''
The nested JSON data contains another JSON object as the value for some of its attributes.
This makes the data multi-level and we need to flatten it as per the project requirements for better readability

'''


def get_params() -> dict:
    try:
        parser = argparse.ArgumentParser(description='DataTest')
        parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
        parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
        parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
        parser.add_argument('--output_location', required=False, default="./output_data/outputs/")

        return vars(parser.parse_args())

    except Exception as e:
        print('get_params  has error')
        raise e


def get_folder_list(params):
    file_path = []
    try:
        for file in os.listdir(params.get('transactions_location')):
            d = os.path.join(params.get('transactions_location'), file)
            if os.path.isdir(d):
                file_path.append(os.path.join(d, 'transactions.json'))

        return file_path

    except Exception as e:
        print('get_folder_list has error')
        raise e

'''
The data contains multiple levels. To convert it to a dataframe we will use the json_normalize() 
function of the pandas library.
'''

def convert_json_to_df(file_path):
    data = pd.DataFrame()
    try:
        for path in file_path:
            if os.path.exists(path):
                with open(path) as datafile:
                    for line in datafile:
                        # json_data = json.loads(line)
                        df1 = pd.json_normalize(json.loads(line), record_path=['basket'],
                                                meta=['customer_id', 'date_of_purchase'])
                        data = pd.concat([data, df1])
            else:
                print('File is not present at this location : ', path)
        return data
    except Exception as e:
        print('convert_json_to_df has error')
        raise e


def create_master_df(transactions_df, customers_df, products_df) -> object:
    try:
        master_df = transactions_df.copy()
        master_df = master_df.merge(customers_df, on='customer_id', how='left')
        master_df = master_df.merge(products_df, on='product_id', how='left')
        master_df['date_of_purchase'] = pd.to_datetime(master_df['date_of_purchase']).dt.date
        master_df.sort_values(by='date_of_purchase')
        ## master_df = master_df.set_index('date_of_purchase')

        return master_df
    
    except Exception as e:
        print('create_master_df has error')
        raise e




def main():
    try:
        params = get_params()
        file_path = get_folder_list(params)
        transactions_df = convert_json_to_df(file_path)
        customers_df = pd.read_csv(params.get('customers_location'))
        products_df = pd.read_csv(params.get('products_location'))
        master_df = create_master_df(transactions_df, customers_df, products_df)
        #master_df.drop(['product_description', 'date_of_purchase'], axis=1, inplace=True)

        var = master_df.groupby(['customer_id', 'loyalty_score', 'product_id', 'product_category', 'product_description'])['price'].describe()[['count']]
        var.rename(columns={'count': 'purchase_count'}, inplace=True)
        var = var.reset_index()
        var.to_csv(os.path.join(params.get('output_location'), 'output_df.csv'))

        jas_data = (var.groupby(['customer_id', 'loyalty_score'])
             .apply(lambda x: x[['product_id', 'product_category', 'product_description', 'purchase_count']].to_dict('records'))
             .reset_index().set_index('customer_id')
             .rename(columns={0: 'product_data'})
             .to_json(orient='index', indent=2))
             #.to_json(orient='records', indent=1))

        file_data = open(os.path.join(params.get('output_location'), 'output_df.json'), 'w')
        file_data.write(jas_data)
        file_data.close()

    except Exception as e:
        print('Exception in main')
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


if __name__ == "__main__":
    main()
