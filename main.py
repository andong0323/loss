#!/usr/bin/env python
#coding:utf8

#standard library
import sys
import os
import configparser
import commands
from redis import StrictRedis
from redis import ConnectionPool
from multiprocessing import Pool
from multiprocessing import Process
import websocket
import json
import logging
import traceback
import time

#user_defined library
import tools.logger
import tools.redis_data
'''
logging.basicConfig(level=logging.DEBUG,\
			format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',\
			datefmt='%a,%d %b %Y %H:%M:%S',\
			filename='debug.log',\
			filemode='w')
'''

def run():
	pid = os.getpid()
	logger = tools.logger.Logger('mainlog','mainlog'+str(pid)+'.log')
	work_path = os.path.abspath(os.curdir)
	config_path = os.path.join(work_path,"conf/loss.conf")
	conf = configparser.ConfigParser()
	conf.read(config_path)
	#data server config
	dataserver_ip = conf.get("dataserver","ip")
	dataserver_port = conf.get("dataserver","port")
	dataserver_key = conf.get("dataserver","key")
	#redis server config
	redis_ip = conf.get("redis","ip")
	redis_port = conf.get("redis","port")
	#currency config
	platform = conf.get("trade","platform")
	currency = conf.get("trade","currency")
	currency_list = currency.split(",")
	#redis pool
	redispool = ConnectionPool(host=redis_ip,port=int(redis_port),db=9)
	myredis = StrictRedis(connection_pool=redispool)
	
	#begin to store data and send trade info
	trade_number = 1
	while 1:
		logger.info("begin trade:"+str(trade_number))
		try:
			trade_process(myredis,logger,platform,currency_list)
		except Exception as e:
			logger.info(e)
			msg = traceback.format_exc()
			logger.info(msg)
		time.sleep(15)
		trade_number += 1

def trade_process(myredis,logger,platform,currency_list):
	logger.info("ready to connect data server")
	ws = websocket.create_connection("ws://47.104.136.5:8201")
	if ws.connected:
		#send authentication info
		ws.send('{"action":"Authentication","key":"D04574F15C4DFA3FF576F8EFC7735A10"}')
		auth_info = ws.recv()
		auth_dict = json.loads(auth_info)
		logger.info(auth_dict)
		#get precision info
		ws.send('{"action":"GetSymbols","platform":"' + platform + '"}')
		precision_info = ws.recv()
		precision_dict = json.loads(precision_info)
		tools.redis_data.store_precision_data(myredis,precision_dict)
		logger.info(precision_dict)#for debug

		#check authentication info
		if auth_dict["msg"]=='认证成功'.decode("utf-8"):
			logger.info("auth success!")
			begin_timestamp = int(time.time())
			
			pid = os.fork()
			if pid==0:#child process
				print 'begin to subscribe data'
				#begin to subscribe currency price info
				#store_data_pool = Pool(len(currency_list))
				#for currency in currency_list:
				#	logger.info("store data of currency:"+currency)
				#	store_data_pool.apply_async(store_data,args=(myredis,logger,ws,platform,currency))
				#store_data(myredis,logger,ws,platform,"ethbtc")
				for currency in currency_list:
					logger.info("store data of currency:"+currency)
					p = Process(target=store_data,args=(myredis,logger,ws,platform,currency))
					p.start()
			else:#parent process ping the server all the time
				plogger = tools.logger.Logger('ppid',"ppid"+str(pid)+'.log')
				ws.send('{"type":"ping"}')
				try:
					while 1:
						end_timestamp = int(time.time())
						if(end_timestamp - begin_timestamp > 8):
							try:
								ws.send('{"type":"ping"}')
								begin_timestamp = end_timestamp
								plogger.info('send heart beat')
							except Exception as e:
								plogger.info(str(e))
				except Exception as e:
					plogger.info(str(e))
		else:
			logger.info("auth fail!")
	else:
		logger.info("fail to connect data server")
		return

def store_data(myredis,logger,ws,platform,currency):
	#subscribe data
	print 'run function store_data'
	logger.info("subscribe currency:"+currency)
	print currency
	send_message = '{"action":"SubMarketKline","symbol":"'+currency+'","platform":"'+platform+'"}'
	print send_message
	ws.send(send_message)
	subscribe_data = ws.recv()
	logger.info(subscribe_data)
	print 'get data'
	while 1:
		data = ws.recv()
		format_data = json.loads(data)
		timestamp = format_data["timestamp"]
		open_price = format_data["open"]
		close_price = format_data["close"]
		high_price = format_data["high"]
		low_price = format_data["low"]
		result = str(timestamp)+"|"+str(open_price)+"|"+str(close_price)+"|"+str(high_price)+"|"+str(low_price)
		print result
		myredis.lpush(currency,result)



if __name__ == "__main__":
	run()
