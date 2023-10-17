import { Controller, Post, Body } from '@nestjs/common';
import { KafkaService } from './kafka.service';

@Controller('kafka')
export class KafkaController {
  constructor(private kafkaService: KafkaService) {}

  @Post('produce')
  async produceToKafka(@Body() payload: { topic: string; message: string }) {
    const { topic, message } = payload;
    console.log({ topic });
    console.log({ message });
    await this.kafkaService.produceMessage(topic, message);
    return { message: 'Mensaje enviado a Kafka' };
  }
}
