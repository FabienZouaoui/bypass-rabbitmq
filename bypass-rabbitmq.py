#!/usr/bin/env python3

import argparse, sys, gc, pika, logging, logging.config, signal, time, asyncio, aio_pika
from functools import partial
from kubernetes import client
from kubernetes import config as kube_config
from kafka import KafkaProducer

MAX_AQUEUE_SIZE = 1000

class Instance:
    def __init__(self, ip, port, name, rabbitmq_queue, producer, topic, aqueue):
        logging.info(f"Instanciating new host {name} with ip {ip}")
        self.ip         = ip
        self.name       = name
        self.port       = port
        self.rmq_queue  = rabbitmq_queue
        self.producer   = producer
        self.topic      = topic
        self.aqueue     = aqueue
        self.active     = True
        self.consuming  = False

    async def start_consuming(self, rabbitmq_prefetch, unsafe):
        logging.debug(f"Connecting to {self.ip}")
        self.connection     = await aio_pika.connect_robust('amqp://' + self.ip + ':' + str(self.port) + '/')
        self.channel        = await self.connection.channel()
        self.q              = await self.channel.get_queue(self.rmq_queue)
        await self.channel.set_qos(prefetch_count=rabbitmq_prefetch)

        logging.debug(f"Starting consumption on {self.name}")
        async with self.q.iterator() as queue_iter:
            async for message in queue_iter:
                if not self.active:
                    return True

                logging.debug(" [x] Received %r" % message.body)
                await self.push_to_kafka(message, unsafe, self.aqueue)


    async def stop_consuming(self):
        logging.info(f"Stopping consumption on {self.name}")
        self.consuming = False
        self.active    = False
        await self.connection.close()

    async def push_to_kafka(self, message, unsafe, aqueue):
        logging.debug("Pushing to kafka")
        future = self.producer.send(self.topic, message.body)
        if unsafe:
            await message.ack()
            return True

        await aqueue.put(Message(message, future))

        
class Message:
    def __init__(self, message, future):
        self.message = message
        self.future  = future

    def is_delivered(self):
        if self.future.succeeded():
            return True
        return False

    def has_failed(self):
        if self.future.failed():
            return True
        return False

    async def ack(self):
        await self.message.ack()

    async def nack(self):
        await self.message.nack()
 

async def check_and_ack(aqueue):
    logging.debug("Starting asyncio queue consumer")
    while True:
        logging.debug("Iterating asyncio queue consumer loop")
        message = await aqueue.get()
        logging.debug("Processing asyncio queue message")

        while not message.future.is_done:
            logging.debug("waiting for message status")
            await asyncio.sleep(0.1)

        if message.is_delivered():
            logging.debug("message successfully delivered to kafka")
            await message.ack()
            continue

        if message.has_failed():
            logging.error("Unable to send message to kafka")
            await message.nack()


async def signal_handler(tasks, instances, producer, v1, signal):
    logging.info(f"signal {signal} received")

    logging.info("Closing kubernetes connection")
    v1.api_client.close()

    for instance in instances:
        await instance.stop_consuming()

    logging.info("Closing publisher")
    producer.close(timeout=10)

    logging.info("Cancelling tasks")
    for task in tasks:
        task.cancel()

async def main(asyncio_queue_size, kafka_brokers, kafka_topic, kafka_batch_size, loop_time, pods_labels, rabbitmq_port, rabbitmq_prefetch, rabbitmq_queue, unsafe, dry_run, debug):
    kube_config.load_incluster_config()
    v1               = client.CoreV1Api()

    producer         = KafkaProducer(bootstrap_servers=kafka_brokers,batch_size=kafka_batch_size,acks=1,compression_type='snappy',api_version=('1.0'))
    tasks            = []
    instances        = []
    log_level        = 'INFO'
    if debug:
        log_level    = 'DEBUG'
    aqueue           = asyncio.Queue(maxsize=asyncio_queue_size)

    #https://docs.python.org/fr/3/library/functools.html#functools.partial
    #https://docs.python.org/3/library/asyncio-eventloop.html#unix-signals
    loop = asyncio.get_running_loop()
    #loop.add_signal_handler(signal.SIGTERM, partial(sigterm_handler, tasks, instances, producer, v1))
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), lambda: asyncio.ensure_future(signal_handler(tasks, instances, producer, v1, signame)))

    # https://stackoverflow.com/questions/7507825/where-is-a-complete-example-of-logging-config-dictconfig
    LOGCONFIG = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'standard': {
                'format': '%(asctime)s %(levelname)s: %(message)s'
            }
        },
        'handlers': {
            'default': {
                'level': log_level,
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout'
            }
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['default'],
                'level': log_level,
                'propagate': False
            }
        }
    }

    logging.config.dictConfig(LOGCONFIG)

    logging.info("Starting application")
    for pod in v1.list_namespaced_pod(namespace="default",label_selector=pods_labels).items:
        instance = Instance(pod.status.pod_ip, rabbitmq_port, pod.metadata.name, rabbitmq_queue, producer, kafka_topic, aqueue)
        instances.append(instance)

        if dry_run:
            logging.info("Should have started consuming {pod.metadata.name}, but dry-run flag is set")
            continue

        tasks.append(
            asyncio.create_task(instance.start_consuming(rabbitmq_prefetch, unsafe), name=instance.name)
        )

    tasks.append(asyncio.create_task(check_and_ack(aqueue), name='queue_worker'))

    for task in tasks:
        await task


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--asyncio-queue-size",   help="Asyncio queue size. Used for acknowledging rmq messages", default=MAX_AQUEUE_SIZE, type=int)

    parser.add_argument("--kafka-batch-size",     help="Kafka batch size", default=100, type=int)
    parser.add_argument("--kafka-brokers",        help="Kafka brokers to produce messages to", default="kafka:9092")
    parser.add_argument("--kafka-topic",          help="Kafka topic", default="requests")

    parser.add_argument("--loop-time",            help="Time in seconds to sleep between pods listing", default=60, type=int)

    parser.add_argument("--pods-labels",          help="Label Filter to apply when listing pods", default="run=sirdata-frontend")

    parser.add_argument("--rabbitmq-port",        help="Port used to contact rabbitmq", default=5672, type=int)
    parser.add_argument("--rabbitmq-prefetch",    help="Rabbitmq message prefetch count", default=1, type=int)
    parser.add_argument("--rabbitmq-queue",       help="Queue name", default="REQUESTS_QUEUE")

    parser.add_argument("--unsafe",               help="Do not enforce message verification on kafka before acknowledging it on rabbitmq", dest='unsafe', action='store_true')
    parser.set_defaults(unsafe=False)

    parser.add_argument("--dry-run",              help="Do not consume messages, just maintain pods list and connect to everything", dest='dry_run', action='store_true')
    parser.add_argument("--no-dry-run",           help="Do the stuff", dest='dry_run', action='store_false')
    parser.set_defaults(dry_run=False)

    parser.add_argument("--debug",                help="print some debug output", action='store_true')
    parser.set_defaults(debug=False)

    args = parser.parse_args()

    asyncio.run(
        main(
            args.asyncio_queue_size,
            args.kafka_brokers,
            args.kafka_topic,
            args.kafka_batch_size,
            args.loop_time,
            args.pods_labels,
            args.rabbitmq_port,
            args.rabbitmq_prefetch,
            args.rabbitmq_queue,
            args.unsafe,
            args.dry_run,
            args.debug,
        )
    )
