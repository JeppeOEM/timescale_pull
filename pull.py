import datetime
from binance.client import Client
import json
import pandas as pd
import websocket #need pip install websocket-client and websocket
import pprint
import psycopg2
from decouple import config

client = Client()

dict_ = client.get_exchange_info()

# [i['symbol'] for i in dict_['symbol']]
sym = [i['symbol'] for i in dict_['symbols'] if i['symbol'].endswith('USDT')]
# filter out forex and up down
print(len(sym))

sym = [i.lower() + '@kline_1m' for i in sym]

stream_these = "/".join(sym)
connection = psycopg2.connect(user="postgres",
                                          password=config("DB"),
                                          host="127.0.0.1",
                                          port="5432",
                                          database="postgres")
cursor = connection.cursor()    


def manipulate(data):
    try:
        value_dict = data['data']['k']
        price, sym = value_dict['c'], value_dict['s']
        event_time = pd.to_datetime([data['data']['E']],unit='ms')
        df = pd.DataFrame([[price,sym]], index=event_time)
    except Exception as e:
        print(e) 

    return df

def on_message(ws, message):
    json_message = json.loads(message)
    df = manipulate(json_message)

    for index, row in df.iterrows():
        query = "INSERT INTO raw_trade_data (TIME, SYMBOL, PRICE, QUANTITY) VALUES (%s, %s, %s, %s)"
        record_to_insert = (index, row[1], row[0],23)
        try:
            cursor.execute(query, record_to_insert)
            connection.commit()
            print("Insertion successful!")
        except psycopg2.Error as e:
            print(f"Error during insertion: {e}")

socket = "wss://stream.binance.com:9443/stream?streams="+stream_these

ws=websocket.WebSocketApp(socket, on_message=on_message)
ws.run_forever()



# {
#   "e": "kline",     // Event type
#   "E": 1672515782136,   // Event time
#   "s": "BNBBTC",    // Symbol
#   "k": {
#     "t": 123400000, // Kline start time
#     "T": 123460000, // Kline close time
#     "s": "BNBBTC",  // Symbol
#     "i": "1m",      // Interval
#     "f": 100,       // First trade ID
#     "L": 200,       // Last trade ID
#     "o": "0.0010",  // Open price
#     "c": "0.0020",  // Close price
#     "h": "0.0025",  // High price
#     "l": "0.0015",  // Low price
#     "v": "1000",    // Base asset volume
#     "n": 100,       // Number of trades
#     "x": false,     // Is this kline closed?
#     "q": "1.0000",  // Quote asset volume
#     "V": "500",     // Taker buy base asset volume
#     "Q": "0.500",   // Taker buy quote asset volume
#     "B": "123456"   // Ignore
#   }
# }