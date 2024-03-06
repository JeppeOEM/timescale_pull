from decouple import config
from binance import ThreadedWebsocketManager
import psycopg2
import datetime
import time

api_key = config("API_KEY")
api_secret = config("API_SECRET")
db_pass = config("DB")

def main():
    symbol = "ETHUSDT"
    max_retries = 5
    retry_delay = 2  # seconds

    for retry_count in range(1, max_retries + 1):
        try:
            connection = psycopg2.connect(user="postgres",
                                          password=config("DB"),
                                          host="127.0.0.1",
                                          port="5432",
                                          database="postgres")
            cursor = connection.cursor()    

            stream = ["ethusdt@trade", "btcusdt@trade"] #pair@sockettype
            twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
            twm.start()

            def handle_socket_message(msg, cursor=cursor):
                msg = msg["data"]
                print(msg)

                query = "INSERT INTO raw_trade_data (TIME, SYMBOL, PRICE, QUANTITY)"+ \
                        " VALUES (%s,%s,%s,%s)"
                timestamp = datetime.datetime.fromtimestamp(int(msg["T"] / 1000))
                record_to_insert = (timestamp, msg["s"], msg["p"], msg["q"])
                cursor.execute(query, record_to_insert)
                print(record_to_insert)
                connection.commit()

            twm.start_multiplex_socket(callback=handle_socket_message, streams=stream)
            twm.join()  # block everything after until the websocket is finished

        except (psycopg2.Error, Exception) as error:
            print(f"Error: {error}")
            if retry_count < max_retries:
                print(f"Retrying in {retry_delay} seconds (attempt {retry_count}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print(f"Max retry attempts reached. Exiting.")
                break
        finally:
            # Close the WebSocket connection
            twm.stop()
            twm.join()

            # Close the database connection
            if connection:
                connection.close()
                print("Database connection closed.")

if __name__ == "__main__":
    main()

#docker exec -it timescaledb psql -U postgres

# SELECT * FROM raw_trade_data
# CREATE TABLE IF NOT EXISTS raw_trade_data (
#    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
#    symbol text NOT NULL,
#    price double PRECISION NOT NULL,
#    quantity double PRECISION NOT NULL
# );

#view table
#\dt
#SELECT create_hypertable('raw_trade_data','time') # time is the timestamp columns name    
#
#    )
# {
#   "e": "trade",     // Event type
#   "E": 123456789,   // Event time
#   "s": "BNBBTC",    // Symbol
#   "t": 12345,       // Trade ID
#   "p": "0.001",     // Price
#   "q": "100",       // Quantity
#   "b": 88,          // Buyer order ID
#   "a": 50,          // Seller order ID
#   "T": 123456785,   // Trade time
#   "m": true,        // Is the buyer the market maker?
#   "M": true         // Ignore
# }