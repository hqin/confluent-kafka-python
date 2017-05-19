#!/usr/bin/env python3
#
# Copyright 2017 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer to work with asyncio, adapted from producer.py.
# Sends few lines to Kafka.
# Requires python 3.5+
#

from confluent_kafka import Producer
from pprint import pprint
import sys
import asyncio
import concurrent.futures
import logging
import time


def delivery_callback(err, msg):
    '''Optional per-message delivery callback (triggered by poll() or flush())
       when a message has been successfully delivered or permanently
       failed delivery (after retries).
    '''

    log = logging.getLogger("delivery_callback")
    if err:
        log.error('%% Message failed delivery: %s\n' % err)
    else:
        log.info('%% Message delivered to %s [%d]\n' % (msg.topic(), msg.partition()))


def create_producer(broker, topic):
    log = logging.getLogger('create_producer')
    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    p = Producer(**conf)

    log.info("created producer {}".format(p))

    # return the producer
    return p


def produce(p, line):
    log = logging.getLogger('produce_task')
    try:
        # Produce line (without newline)
        p.produce(topic, line.rstrip(), callback=delivery_callback)

    except BufferError as e:
            log.error('%% Local producer queue is full '
                      '(%d messages awaiting delivery): try again\n' %
                      len(p))

    # Serve delivery callback queue.
    # NOTE: Since produce() is an asynchronous API this poll() call
    #       will most likely not serve the delivery callback for the
    #       last produce()d message.
    p.poll(0)


def producer_flush(p):
    p.flush()


async def run_producer_tasks(executor, broker, topic):
    log = logging.getLogger('run_producer_tasks')
    log.info('starting')

    log.info('creating executor tasks')
    loop = asyncio.get_event_loop()
    task = loop.run_in_executor(executor, create_producer, broker, topic)
    log.info('waiting for creatign producer')
    completed, pending = await asyncio.wait([task])
    p = [f.result() for f in completed][0]

    for line in ('line 1', 'line 2', 'line 3'):
        task = loop.run_in_executor(executor, produce, p, line)
        log.info('produce msg: ({})'.format(line))
        completed, pending = await asyncio.wait([task])
        log.info('produced msg: ({})'.format(line))

    # Wait until all messages have been delivered
    log.info('%% Waiting for %d deliveries\n' % len(p))
    task = loop.run_in_executor(executor, producer_flush, p)
    completed, pending = await asyncio.wait([task])
    log.info("producer flushed")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Configure logging to show the name of the thread
    # where the log message originates.
    logging.basicConfig(
        level=logging.INFO,
        format='%(threadName)10s %(name)18s: %(message)s',
        stream=sys.stderr,
    )

    # Create a limited thread pool.
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=2,
    )

    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(
            run_producer_tasks(executor, broker, topic)
        )
    finally:
        event_loop.close()

    sys.exit(0)
