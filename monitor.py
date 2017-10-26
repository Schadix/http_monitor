import requests
import time
from requests import HTTPError, Timeout, ConnectionError
import logging.handlers
from datetime import datetime
import boto3
import botocore
import queue
import threading
import backoff
import os

HTTP_REQUEST_FREQUENCY_SECONDS = 5

cw = boto3.client('cloudwatch')

num_worker_threads = 1

LOG_FILENAME = "comcast-v1--output.log"
logger = logging.getLogger('RotatingLogger')
logger.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000000, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(filename)s - %(levelname)s - %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("starting")

cw_namespace = 'TEST_MODEM_PI_HTTP_MONITOR'
# if os.environ.get('CW_NAMESPACE'):
#     cw_namespace = os.environ['CW_NAMESPACE']


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException, botocore.exceptions.EndpointConnectionError))
def put_cloudwatch_data(namespace, metric_data):
    logger.debug("put_cloudwatch_data - namespace: {}".format(namespace))
    for d in metric_data:
        logger.debug("put_cloudwatch_data - metric_name: {}, value: {}".format(d['MetricName'], d['Value']))
    return cw.put_metric_data(Namespace=namespace, MetricData=metric_data)


def worker():
    while True:
        cw_params = q.get()
        if cw_params is None:
            break
        try:
            put_cloudwatch_data(namespace=cw_namespace, metric_data=cw_params)
            q.task_done()
        except Exception as exception:
            logger.error(exception)


q = queue.Queue()
threads = []
for i in range(num_worker_threads):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)


def generate_cw_message(time_in_utc, success_count, failure_count, latency):
    logger.info("generate_cw_message. success: {}, failure: {}, latency: {}"
                .format(success_count, failure_count, latency))
    if failure_count > 0 and success_count < 1:
        param = [{
            'MetricName': 'failure',
            'Dimensions': [
                {
                    'Name': 'http_monitor',
                    'Value': 'pi'
                },
            ],
            'Timestamp': time_in_utc,
            'Value': failure_count,
            'Unit': 'Count'
        }]
    else:
        param = [
            {
                'MetricName': 'success',
                'Dimensions': [
                    {
                        'Name': 'http_monitor',
                        'Value': 'pi'
                    },
                ],
                'Timestamp': time_in_utc,
                'Value': success_count,
                'Unit': 'Count'
            },
            {
                'MetricName': 'failure',
                'Dimensions': [
                    {
                        'Name': 'http_monitor',
                        'Value': 'pi'
                    },
                ],
                'Timestamp': time_in_utc,
                'Value': failure_count,
                'Unit': 'Count'
            },
            {
                'MetricName': 'latency',
                'Dimensions': [
                    {
                        'Name': 'http_monitor',
                        'Value': 'pi'
                    },
                ],
                'Timestamp': time_in_utc,
                'Value': latency,
                'Unit': 'Milliseconds'
            }

        ]
    return param


while True:
    try:
        time.sleep(HTTP_REQUEST_FREQUENCY_SECONDS)
        logger.debug("queue size: {}".format(q.qsize()))
        r = requests.get("http://www.google.com", timeout=5)
        time_utc = datetime.utcnow()
        output_text = "{},{},{},{}".format(time_utc, r.status_code, r.reason, r.elapsed)

        q.put(generate_cw_message(time_utc, 1, 0, r.elapsed.total_seconds() * 1000))
        logger.info(output_text)
    except (HTTPError, Timeout, ConnectionError) as inst:
        q.put(generate_cw_message(0, 1, 0))
        logger.error(inst)
    except Exception as e:
        q.put(generate_cw_message(0, 1, 0))
        # block until all tasks are done
        # q.join()
        logger.error(e)
        # stop workers
        # for i in range(num_worker_threads):
        #     q.put(None)
        # for t in threads:
        #     t.join()


