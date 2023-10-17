/* eslint-disable prettier/prettier */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable prettier/prettier */
import { Injectable } from '@nestjs/common';
import * as Kafka from 'node-rdkafka';

@Injectable()
export class KafkaService {
  private producer: Kafka.Producer = null;
  constructor() {
    const kafkaBroker = process.env.KAFKA_BROKERS;
    const clientId = process.env.KAFKA_CLIENT_ID;
    const user = process.env.KAFKA_API_KEY;
    const pwd = process.env.KAFKA_API_SECRET;
    this.producer = new Kafka.Producer({
      'dr_cb': true,
      log_level: 1,
      "client.id": clientId,
      "metadata.broker.list": `${kafkaBroker}`,
      "sasl.mechanisms": "PLAIN",
      "sasl.username": user,
      "sasl.password": pwd,
      "security.protocol": "sasl_ssl",
      "socket.keepalive.enable": true,
      "socket.blocking.max.ms": 100,
      "socket.nagle.disable": false,
    });

    this.producer.connect();
    this.producer.on('ready', () => {
      console.log('Producer is ready');
    });
  }

  async produceMessage(topic: string, message: string) {
    this.producer.produce(topic, -1, Buffer.from(message));
  }
}
