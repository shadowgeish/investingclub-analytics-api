from flask import Flask, render_template, url_for, copy_current_request_context
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from flask_socketio import SocketIO, emit
from threading import Thread, Event
from datetime import datetime
from time import sleep
import random


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
app.config['DEBUG'] = True

# Set this variable to "threading", "eventlet" or "gevent" to test the
# different async modes, or leave it set to None for the application to choose
# the best option based on installed packages.
async_mode = None
real_time_price = dict()
thread = Thread()

# socket_io = SocketIO(app, async_mode=async_mode)
socket_io = SocketIO(app, async_mode=async_mode, logger=True, engineio_logger=True)

@app.route('/')
def index():
    #only by sending this page first will the client be connected to the socketio instance
    return render_template('index.html')

def live_stock_prices():

    dtt = datetime.now()
    dtt_s = datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=8, minute = 30)
    dtt_e = datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=23, minute=00)

    socket_io.emit('last_traded_price', 3, namespace='/stock_prices')

    print('Date check {} < {} < {} '.format(dtt_s, dtt, dtt_e))

    while True:
        if dtt_s < dtt < dtt_e:
            print('Date check {} < {} < {} '.format(dtt_s,dtt,dtt_e))

            import pandas as pd
            import pandas as pd
            import requests
            import json

            ddf = pd.read_csv("../asset_prices/stock_universe.csv", sep=',', keep_default_na=False)
            ddf['full_code'] = ddf['Code'] + '.' + ddf['Exchange']
            lstock = ddf['full_code'].tolist()  # ddf.to_dict(orient='records')
            collection_name = "stocks"
            db_name = "asset_analytics"
            sublists = [lstock[x:x + 20] for x in range(0, len(lstock), 20)]
            stringlist = []
            for subset in sublists:
                stringlist.append(','.join(subset))
            for sublist in stringlist:
                print('Getting data for sub string {}'.format(sublist))
                sreq = "https://eodhistoricaldata.com/api/real-time/CAC.PA?api_token=60241295a5b4c3.00921778&fmt=json&s={}"
                req = requests.get(sreq.format(sublist))
                list_closing_prices = req.json()
                print('Reveived data {}'.format(sublist))
                # Real time prices
                global real_time_price
                if real_time_price is None:
                    real_time_price = dict()

                for price in list_closing_prices:
                    code = price['code']
                    key = price['code']  # + '_' + request_day
                    # push the price data to the socket so they could quicky see it
                    print('sending data for {}'.format(price))
                    socket_io.emit('last_traded_price', price, namespace='/stock_prices')

                    # push to the data price base for that code
                    # DATA BASE
                    if key not in real_time_price.keys():
                        real_time_price[key] = list()
                        # read from db if there is historic for the day to real_time_price
                        # READ DATA BASE
                    current_list = real_time_price[key]
                    exists = 0
                    for item in current_list:
                        if item['timestamp'] == price['timestamp']:
                            exists = 1
                            break
                    if exists == 0:
                        real_time_price[key].append(price)

                    # push the json array of the day to the socket
                    print('Sending intraday prices {}'.format(real_time_price[key]))
                    socket_io.emit('intraday_traded_prices', real_time_price[key], namespace='/stock_prices')


                # print(real_time_price)

            #number = random.random() * 10
            #print(number)
            #socket_io.emit('newnumber', {'number': number}, namespace='/stock_prices')
            sleep(10)
        else:
            real_time_price = dict()


if __name__ == '__main__':
    # app.run(debug=True, port=5001
    print('Starting main')
    # global thread
    thread = socket_io.start_background_task(target=live_stock_prices)
    socket_io.run(app, debug=True, host='localhost', port=5000)