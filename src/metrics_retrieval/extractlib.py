#!/usr/bin/env python

import os
import json, csv
import requests
from datetime import datetime



JAEGER_TRACES_ENDPOINT = "http://127.0.0.1:8001/jaeger/api/traces?lookback=custom&maxDuration&minDuration&" #"http://10.98.88.252:80/jaeger/api/traces?end=1704974460000000&start=1704974400000000&"
JAEGER_TRACES_PARAMS = "service="
LIMIT =  800000  # 0 if no limit

if LIMIT > 0:
    JAEGER_TRACES_ENDPOINT = JAEGER_TRACES_ENDPOINT + "limit=" + str(LIMIT) + "&"

def get_traces(service, end_unix, start_unix):
    """
    Returns list of all traces for a service
    """
    url = JAEGER_TRACES_ENDPOINT + "end=" + end_unix + "&start=" + start_unix +  "&" +  JAEGER_TRACES_PARAMS + service
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise err
    response = json.loads(response.text)
    traces = response["data"]
    return traces

JAEGER_SERVICES_ENDPOINT = "http://127.0.0.1:8001/jaeger/api/services"

def get_services():
    """
    Returns list of all services
    """
    try:
        response = requests.get(JAEGER_SERVICES_ENDPOINT)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise err
        
    response = json.loads(response.text)
    services = response["data"]
    return services

def write_traces(directory, traces):
    """
    Write traces locally to files
    """
    for trace in traces:
        traceid = trace["traceID"]
        path = directory + "/" + traceid + ".json"
        with open(path, 'w') as fd:
            fd.write(json.dumps(trace))

def extract_data(span):

    operation_name_parts = span["operationName"].split(".")
    operation_name = operation_name_parts[0] if operation_name_parts else ""
    return {
        "trace_id": span["traceID"],
        "span_id": span["spanID"],
        "start_time": span["startTime"],
        "duration": span["duration"],
        "client": next((tag["value"] for tag in span["tags"] if tag["key"] == "istio.canonical_service"), ""),
        "server": operation_name,
        "grpc_path": next((tag["value"] for tag in span["tags"] if tag["key"] == "grpc.path"), ""),
        "http_status_code": next((tag["value"] for tag in span["tags"] if tag["key"] == "http.status_code"), ""),
        "grpc_message": next((tag["value"] for tag in span["tags"] if tag["key"] == "grpc.message"), ""),
        "http_method": next((tag["value"] for tag in span["tags"] if tag["key"] == "http.method"), ""),
        "response_size": next((tag["value"] for tag in span["tags"] if tag["key"] == "response_size"), ""),
        "request_size": next((tag["value"] for tag in span["tags"] if tag["key"] == "request_size"), ""),
        "node_id": next((tag["value"] for tag in span["tags"] if tag["key"] == "node_id"), ""),
    }


def json_to_csv(traces, csv_filename):
    header = ["trace_id", "span_id", "start_time", "duration", "client", "server", "grpc_path", "http_status_code",
             "grpc_message", "http_method", "response_size", "request_size", "node_id"]

    with open(csv_filename, mode="a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header)
        #writer.writeheader()

        for trace in traces:
            for span in trace["spans"]:
                data = extract_data(span)
                writer.writerow(data)
