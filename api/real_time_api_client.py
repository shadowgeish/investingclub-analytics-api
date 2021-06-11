from flask import Flask, render_template, url_for, copy_current_request_context
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from flask_socketio import SocketIO, emit
from threading import Thread, Event
from datetime import datetime
from time import sleep
import random


import socketio

# standard Python
sio = socketio.Client(engineio_logger=True)

@sio.on('connect', namespace='/stock_prices')
def on_connect():
    print("I'm connected to the /stock_prices namespace!")

@sio.event
def last_traded_price(data):
    print('I received a last_traded_price message!')

@sio.event
def intraday_traded_prices(data):
    print('I received a intraday_traded_prices message!')


if __name__ == '__main__':
    # app.run(debug=True, port=5001
    print('Starting main')
    sio.connect('http://localhost:5001')
    print('my sid is', sio.sid)