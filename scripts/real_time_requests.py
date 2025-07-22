import string
import csv
import requests
import numpy as np
import sys
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import argparse
import json
import subprocess
from pathlib import Path
from src.metrics_retrieval.extractlib import *

BATCH_SIZE = 15
ROOT = Path(__file__).resolve().parents[1]
data_path = ROOT / "data" / "new_dataset" 


def send_requests(uri, data,i):
    #j = 0
    result = ''
    #while j < num_reqs: 
    start_req_time = time.time()
    req = requests.post(url = uri, json = data)
    if req.status_code == 200:
        req_time = time.time() - start_req_time
        result = f'{start_req_time},{req_time},{i}\n'
    return result

def write_csv_continous(data, file_path):
	with open(file_path,'a', newline = '') as out_file:
		writer = csv.writer(out_file, quoting=csv.QUOTE_NONE, escapechar=' ')
		writer.writerow(data)
	#res_file.write('st,ttr,rid\n')


if __name__ == '__main__':
	
	#tunnel = tunnel_open()
	#time.sleep(5)

	parser = argparse.ArgumentParser(description="HTTP Request Generator with fixed rps", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("-r", "--rps", type=float, help="NORMALIZED request per second",required=True)

	# try to find a way to make this into a fixed ssh tunnel port
	parser.add_argument("-u", "--uri", help="HTTP server endpoint",required=True)
	#uri = "http://127.0.0.1:8000/"
	# parser under here is quite useless since we want to keep sending requests
	#parser.add_argument("-n", "--nreq", type=int, help="total number of requests",required=True)
	parser.add_argument("-m","--max-thread",default=10,type=int ,help="Max number of concurrent requests")
	parser.add_argument("-s", "--srep", type=float, help="replicas per microservice",required=True)
	parser.add_argument("-d", "--sd", type=float, help="slowdown factor",required=True)
	args = parser.parse_args()
	config = vars(args)
	rps=config["rps"]
	rep=config["srep"]
	sd=config["sd"]
	#nreqs=config["nreq"]
	uri=config["uri"]
	max_thread=config["max_thread"]

	print(uri)
    
	rps = rps / sd  # denormalize rps, accounting for sd
    

    # log also request to get quantile info
    
	ttr_path = str(data_path) + "/ttr_rep" + str(rep) + "_sd" + str(sd)

	if not os.path.exists(ttr_path):
		os.makedirs(ttr_path)
   
	services = get_services()
	services_target = {"frontend.istio-dt"} 
	all_services = {"frontend.istio-dt","bill.istio-dt","auth.istio-dt",
					"ms1.istio-dt","ms2.istio-dt"}
	services = list(set(services).intersection(services_target))


	printservices = 1
	if printservices == 1:
		if len(services) == 1:
			print("There is " + str(len(services)) +" available service: ")
		else:
			print("There are " + str(len(services)) +" available services: ")
		for service in services:
			print("\t -" + service)

	for service in services:
		path = str(data_path) + f"/{service}" + "/traces_replica" + str(rep)	
		#if not os.path.exists(path):
		#	os.makedirs(path)

	res_file = str(ttr_path) + f'/req_logs_rep{rep}_rps{rps}_nreq{BATCH_SIZE}.csv'


	with open(res_file, mode="w", newline="") as csv_file:
		csv_file.write("st,ttr,rid\n")


	csv_filename = str(data_path) + "/traces_nt_" + "ora" + "_" + "dopo" + "_rps" + str(rps) + ".csv"
		
	header = ["trace_id", "span_id", "start_time", "duration", "client", "server", "grpc_path", "http_status_code",
		"grpc_message", "http_method", "response_size", "request_size", "node_id"]
		
		
	with open(csv_filename, mode="w", newline="") as csv_file:
		writer = csv.DictWriter(csv_file, fieldnames=header)
		writer.writeheader()


    # generate arrival time
	rv = np.random.exponential(1 / rps, size=BATCH_SIZE+1)
	print(f'Target rps: {rps}')

	i = 0
	res = []


	while i < BATCH_SIZE:
		i = i + 1
		start = time.time()

		data = json.dumps({"user_id" : "Fabrizioilfrescodizona"})
		data = json.loads(data)

		with ProcessPoolExecutor(max_workers=max_thread) as executor:
			res.append(executor.submit(send_requests, uri, data, i))
			time.sleep(rv[i])

		elapsed_time = time.time() - start

		time.sleep(5)
		r_string= res[-1].result()
		print(r_string.rstrip())
		write_csv_continous([r_string.rstrip()], res_file)

		
		end = time.time()
			
		print(f'Took {elapsed_time}')
		
		
		end_unix = str(int(end * 1e6))        # Jaeger needs timestamp in microseconds format
		start_unix = str(int(start * 1e6))

		
		print(f"end_unix: {end_unix}")
		print(f"start_unix: {start_unix}")
		

		for service in services:
			traces = get_traces(service, end_unix, start_unix)
		
		json_to_csv(traces, csv_filename)
    
	with open(res_file, "r", newline='') as out_file:
		lines = out_file.readlines()
		lines = [line.replace(' ,', ',') for line in lines]

	with open(res_file, "w", newline='') as out_file:
		[out_file.write(line) for line in lines]

