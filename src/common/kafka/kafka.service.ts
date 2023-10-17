/* eslint-disable prettier/prettier */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable prettier/prettier */
import { Injectable } from '@nestjs/common';
import * as Kafka from 'node-rdkafka';

@Injectable()
export class KafkaService {
  private producer: Kafka.Producer = null;
  private _isReady: boolean = false;
  private _isConnected = false;
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
      //"security.protocol": "sasl_ssl",
      "socket.keepalive.enable": true,
      "socket.blocking.max.ms": 100,
      "socket.nagle.disable": false,
    });

    this.producer.on('event.error', (err) => {
      console.error('Error from producer');
      console.error(err);
    })
  }

  async connect(): Promise<void> {
    try {
      this.producer.connect();
      this._isConnected = true;
      this.producer.on("ready", () => {
        this._isReady = true;
      });
      this.producer.on("event.error", (err) => {
        console.error("Error from producer:", err);
      });
    } catch (err) {
      console.error(err);
    }
  }
  
  async produceMessage(topic: string, message: string) {
    this.connect();
    this.producer.on('ready', () => {
      console.log('Producer is ready');
      this._isReady = true;
    });
    if (!this._isReady) {
      console.log("Producer is not ready");
    }else{
      this.producer.produce(topic, -1, Buffer.from(message));
      this.producer.setPollInterval(100);
    }
  }
}
