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
  'SELECT * FROM tezos_wallets'
)
record = cur.fetchall()
season0_df = pd.DataFrame(record)
print(season0_df.head())

# RETRIEVE OFFERS FOR GIVEN NFT
query = """
query MyQuery {
  offer(where: {fa_contract: {_eq: "KT1Rxr3aE9f7p2159VQCowzxQTkuc4QR1iu1"}, status: {_eq: "active"}, token: {token_id: {_eq: "3"}}}) {
    id
    price
    buyer_address
    buyer {
      alias
    }
  }
}
"""

url = "https://data.objkt.com/v2/graphql/"
r = requests.post(url, json={'query': query})
json_data = r.json()
df_data = json_data['data']['offer']
tezos_df = pd.DataFrame(df_data )

print(tezos_df.head())



