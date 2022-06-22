
from tkinter.tix import COLUMN
import requests
import json
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    port=os.getenv('DB_PORT')
)

cur = conn.cursor()

#RETRIEVE SEASON 0 PASS HOLDERS TEZOS WALLETS
cur.execute(
  'SELECT tezos_wallet FROM tezos_wallets'
)
record = cur.fetchall()
season0_df = pd.DataFrame(record, columns=['wallet_address'])


# RETRIEVE OFFERS FOR GIVEN NFT
# query MyQuery {
#  offer(where: {fa_contract: {_eq: "KT1Rxr3aE9f7p2159VQCowzxQTkuc4QR1iu1"}, status: {_eq: "active"}, token: {token_id: {_eq: "3"}}}) {
#    id
#    price
#    buyer_address
#    buyer {
#      alias
#    }
#  }
#}
query = """
query MyQuery {
  offer(where: {fa_contract: {_eq: "KT1X9fyEnL7r2kSsbiYLbm8aNw2C78Af6mKv"}, status: {_eq: "active"}, token: {token_id: {_eq: "3"}}}, order_by: {timestamp: asc_nulls_last}) {
    id
    price
    buyer_address
    buyer {
      alias
      tzdomain
    }
    timestamp
  }
}

"""

url = "https://data.objkt.com/v2/graphql/"
r = requests.post(url, json={'query': query})
json_data = r.json()
print(json_data)
df_data = json_data['data']['offer']
tezos_df = pd.DataFrame(df_data, columns=['buyer_address','buyer', 'tzdomain', 'price', 'timestamp'])
tezos_df['price'] = tezos_df['price'].apply(lambda x: x/1000000)
tezos_df['tzdomain'] = tezos_df['buyer'].apply(lambda x: x['tzdomain'])
tezos_df['buyer'] = tezos_df['buyer'].apply(lambda x: x['alias'])

tezos_df.rename(columns = {'buyer_address':'wallet_address'},inplace=True)

#test database with one wallet in common
#tezos_df = pd.DataFrame({
#  'wallet_address': ['tz1dqkxxmq2w5g6jzJRndFJY9E3gUdioKYK1','tz1QFffX69UydB6Dfzbn8b5xnUoKsGhtY9e7', 'tz1TWU7UjbF1knsgd4dhkVGPPtnkK4wHukVW'],
#   'alias': ['nauti','kees','ks1']})


tezos_df.sort_index(inplace=True)
print("✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸ offers  ✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸")
print(tezos_df.head(99))
season0_df.sort_index(inplace=True)
print("✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸ holders wallets ✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸")
print(season0_df.head(99))

merged = season0_df.merge(tezos_df, indicator=True, how='outer')
merged[merged['_merge']== 'right_only'] 


merged = merged[merged['_merge'].str.contains('both')]
merged = pd.DataFrame(merged, columns=['wallet_address','buyer','tzdomain', 'timestamp'])
print("✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸ Common wallets ✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸✸")
print(merged.head(99))


