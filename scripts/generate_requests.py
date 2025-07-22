import string
import requests
import numpy as np
import sys
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import argparse
import json
from pathlib import Path
from src.metrics_retrieval.extractlib import *


ROOT = Path(__file__).resolve().parents[1]
print(ROOT)
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


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="HTTP Request Generator with fixed rps", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-r", "--rps", type=float, help="NORMALIZED request per second",required=True)
    parser.add_argument("-u", "--uri", help="HTTP server endpoint",required=True)
    parser.add_argument("-n", "--nreq", type=int, help="total number of requests",required=True)
    parser.add_argument("-m","--max-thread",default=10,type=int ,help="Max number of concurrent requests")
    parser.add_argument("-s", "--srep", type=float, help="replicas per microservice",required=True)
    parser.add_argument("-d", "--sd", type=float, help="slowdown factor",required=True)
    args = parser.parse_args()
    config = vars(args)
    rps=config["rps"]
    rep=config["srep"]
    sd=config["sd"]
    nreqs=config["nreq"]
    uri=config["uri"]
    max_thread=config["max_thread"]

    print(uri)
    
    rps = rps / sd  # denormalize rps, accounting for sd
    
    # i = 0
    start = time.time()
    # log also request to get quantile info
    
    ttr_path = str(data_path) + "/ttr_rep" + str(rep) + "_sd" + str(sd)

    if not os.path.exists(ttr_path):
        os.makedirs(ttr_path)
    
    res_file = open(str(data_path) + f'/ttr_rep{rep}_sd{sd}/req_logs_{start}_rep{rep}_rps{rps}_nreq{nreqs}.csv', 'w')
    res_file.write('st,ttr,rid\n')
    # close results file
    res_name = res_file.name


    # generate arrival time
    rv = np.random.exponential(1 / rps, size=nreqs)
    print(f'Target rps: {rps}')

    i = 0
    res = []

    data = json.dumps({"user_id" : "Fabrizioilfrescodizona"})
    data = json.loads(data)

    with ProcessPoolExecutor(max_workers=max_thread) as executor:
        while i < nreqs:
            res.append(executor.submit(send_requests, uri, data, i))
            time.sleep(rv[i])
            i+=1

    elapsed_time = time.time() - start
    time.sleep(5)
    for r in res:
        r_string = r.result()
        res_file.write(r_string)

    res_file.close()
    
    end = time.time()
        
    print(f'Took {elapsed_time}')

    ds = pd.read_csv(res_name)
    ds.ttr *= 1E3
    print(f"Requests {len(ds['ttr'])} mean: {ds['ttr'].mean()} ms std: {ds['ttr'].std()} ms")
    print(f"Median: {ds['ttr'].median()} ms")
    print("Percentage of requests within ms")
    for q in [.5, .66, .75, .80, .90, .95, .98, .99, 1.0]:
        ttr_percentile = ds['ttr'].quantile(q)
        print(f'{q * 100}% {round(ttr_percentile)}')

    
    # Extract traces
    
    end_unix = str(int(end * 1e6))        # Jaeger needs timestamp in microseconds format
    start_unix = str(int(start * 1e6))

    
    print(f"end_unix: {end_unix}")
    print(f"start_unix: {start_unix}")

    services = get_services()
    services_target = {"interface.istio-dt"} 
    all_services = {"adservice.istio-dt","cartservice.istio-dt","checkoutservice.istio-dt",
                    "currencyservice.istio-dt","emailservice.istio-dt",
                    "frontend.istio-dt","paymentservice.istio-dt",
                    "productcatalogservice.istio-dt","recommendationservice.istio-dt"}
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
        path = data_path + f"/{service}" + "/traces_replica" + str(rep)
        if not os.path.exists(path):
            os.makedirs(path)
        traces = get_traces(service, end_unix, start_unix)

        csv_filename = path + "/traces_nt_" + start_unix + "_" + end_unix + "_rps" + str(rps) + ".csv"

        json_to_csv(traces, csv_filename)
        #write_traces(service, traces)
    
