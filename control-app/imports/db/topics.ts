import { Mongo } from 'meteor/mongo';

export interface Topic {
  _id: string;
  region: string;
  accountId: string;
  name: string;
  // attributes: Record<string,string>;
  tags: Record<string,string>;
  createdAt: Date;
  modifiedAt: Date;

  config: {
    DisplayName: string;
    Policy: string;
    EffectiveDeliveryPolicy: string;
    LambdaSuccessFeedbackSampleRate: number;
    FirehoseSuccessFeedbackSampleRate: number;
    SQSSuccessFeedbackSampleRate: number;
    HTTPSuccessFeedbackSampleRate: number;
    ApplicationSuccessFeedbackSampleRate: number;
    FifoTopic: boolean;
    ContentBasedDeduplication: boolean;
  };
}

export const TopicsCollection = new Mongo.Collection<Topic>('Topics');
