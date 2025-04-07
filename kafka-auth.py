import argparse
from confluent_kafka import Producer
import avro.io
import io
import datetime
import time
import ipaddress
import random
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("us-east-1")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000
    


# in-line schema for netflow
schema_inline = '''{ 
    "namespace": "netflow.avro",
    "type": "record",
    "name": "netflow",
    "fields": [
      { "name": "event_type", "type": "string"},
      { "name": "peer_ip_src", "type": "string"},
      { "name": "ip_src", "type": "string"},
      { "name": "ip_dst", "type": "string"},
      { "name": "port_src", "type": "long"},
      { "name": "port_dst", "type": "long"},
      { "name": "tcp_flags", "type": "long"},
      { "name": "ip_proto", "type": "string"},
      { "name": "timestamp_start", "type": "string"},
      { "name": "timestamp_end", "type": "string"},
      { "name": "timestamp_arrival", "type": "string"},
      { "name": "export_proto_seqno", "type": "long"},
      { "name": "export_proto_version", "type": "long"},
      { "name": "packets", "type": "long"},
      { "name": "flows", "type": "long"},
      { "name": "bytes", "type": "long"},
      { "name": "writer_id", "type": "string"}
    ]
  }'''

# broker information
#bserver = 'b-1.demo.6ehtqz.c23.kafka.us-east-1.amazonaws.com:9098,b-2.demo.6ehtqz.c23.kafka.us-east-1.amazonaws.com:9098,b-3.demo.6ehtqz.c23.kafka.us-east-1.amazonaws.com:9098'

# Base Data template
base_data = {
    'event_type': 'purge',
    "peer_ip_src": "192.168.0.1",
    "ip_src": "127.0.0.1",
    "ip_dst": "10.124.7.1",
    "port_src": 80,
    "port_dst": 8080,
    "tcp_flags": 1,
    "ip_proto": "udp",
    "timestamp_start": "2021-07-15 19:35:23.000000",
    "timestamp_end": "2021-11-25 20:20:12.382551",
    "timestamp_arrival": "2021-11-25 20:20:12.382551",
    "export_proto_seqno":  3616409109,
    "export_proto_version": 10,
    "flows": 1,
    "packets": 1,
    "bytes": 80,
    "writer_id": "nflw-avroec01"
}

# For seed data
timedelay = datetime.timedelta(0, 1)
eventtype = ['purge']
protocol = ['tcp', 'udp', 'icmp']   # change based on your need
portdata = [20, 21, 22, 23, 53, 80, 194, 443, 989, 990, 8080]
ipdata = []

kINTERNAL_IPRANGES = ['10.34.0.0/16',
                      '10.24.25.0/24',
                      '11.64.0.0/15',
                      '10.115.19.144/28',
                      '172.17.129.48/30',
                      '172.23.129.224/28',
                      '172.20.193.96/28']

kEXTERNAL_IPRANGES = ['1.46.0.0/19',
                      '23.221.80.0/20',
                      '58.8.0.0/14',
                      '1.186.0.0/15',
                      '2.16.89.0/24']

for iprange in kINTERNAL_IPRANGES + kEXTERNAL_IPRANGES:
    ipdata += [str(ip) for ip in list(ipaddress.ip_network(iprange).hosts())]

def get_random_ip():
    return random.choice(ipdata)

# Avro encoding setup using the in-line schema
schema = avro.schema.parse(schema_inline)
writer = avro.io.DatumWriter(schema)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Generate simulated NetFlow streams')
    parser.add_argument('--topic-name', dest='topic', action='store',
                        default=None, help='netflow', required=True)
    parser.add_argument('--client-id', dest='clientid', action='store',
                        default=None, help='Kafka-P01', required=True)
    parser.add_argument('--cycle', dest='cy', action='store', type=int,
                        default=1, help='Number if iteration [1-100]', required=False,)
    parser.add_argument("-c", "--count", type=int, default=10,
                        help="Number of messages to send")
    parser.add_argument('--broker', dest='bserver', action='store',
                        default=None, help='netflow', required=True)

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()

    # Producer Definition
    producer_config = {
        'bootstrap.servers': args.bserver,
        'batch.size': 262144, # num of bytes
        'linger.ms': 10, # after 10 milisec send the package
        'acks': 1,
        'client.id': args.clientid,
        'compression.codec': 'snappy',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': oauth_cb,
    }

    producer = Producer(producer_config)

    # topic info from argument
    topic = args.topic
    msgCount = args.count
    messageAr = []

    print('Start of data gen - ', str(datetime.datetime.now()))

    # generate the events and convert into snappy object and create an array
    for i in range(msgCount):
        data = base_data
        data["event_type"] = random.choice(eventtype)
        event = data["event_type"]
        data["peer_ip_src"] = get_random_ip()
        data["ip_src"] = get_random_ip()
        data["ip_dst"] = get_random_ip()
        data["port_src"] = random.choice(portdata)
        data["port_dst"] = random.choice(portdata)
        data["ip_proto"] = random.choice(protocol)
        timedata = datetime.datetime.now()
        data["timestamp_start"] = str(timedata - timedelay)
        data["timestamp_end"] = str(timedata)
        data["timestamp_arrival"] = str(timedata + timedelay)
        data["export_proto_seqno"] = random.choice(portdata)
        data["bytes"] = random.randint(1, 1000)
        data["packets"] = random.randint(1, 100)
        data["tcp_flags"] = random.randint(0, 255)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        raw_bytes = bytes_writer.getvalue()
        messageAr.append(raw_bytes)

    print('End of data gen - ', str(datetime.datetime.now()), f'{len(messageAr)}')
    cy = args.cy

    def flush(producer):
        producer.flush()
        

    # Make this run forever
    print(f'Start of data pub: topic={topic} time={str(datetime.datetime.now())}')
    cycled = 0
    while 1:
        if cy != 0 and cycled >= cy:
            print('End of data pub - ', str(datetime.datetime.now()))
            break

        cycled += 1
        messages_to_retry = 0
        i = 0
        for msg in messageAr:
            try:
                i = i + 1
                #print('Start of producer.send - ', str(datetime.datetime.now()))
                producer.produce(topic=topic, value=msg)
                if((i % 100000) == 0):
                    flush(producer)
                    print('End of producer flush - ', str(datetime.datetime.now()))
                    i = 0
            except BufferError as e:
                messages_to_retry += 1

        for msg in messageAr[:messages_to_retry]:
            producer.poll(0)  # check if kafka is there
            try:
                producer.produce(topic=topic, value=msg)
            except BufferError as e:
                producer.poll(0)
                producer.produce(topic=topic, value=msg)

        flush(producer)