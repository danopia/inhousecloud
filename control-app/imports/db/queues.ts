import { Mongo } from 'meteor/mongo';

export interface Queue {
  _id: string;
  region: string;
  accountId: string;
  name: string;
  // attributes: Record<string,string>;
  tags: Record<string,string>;
  createdAt: Date;
  modifiedAt: Date;

  messagesActive: number;
  messagesVisible: number;
  messagesDelayed: number;
  messagesNotVisible: number;

  // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html
  config: {
    "Policy": string; // a whole-ass IAM document
    "RedrivePolicy": string; // deadLetterTargetArn, maxReceiveCount
    // RedriveAllowPolicy
    "DelaySeconds": number;
    "MaximumMessageSize": number;
    "MessageRetentionPeriod": number;
    "ReceiveMessageWaitTimeSeconds": number;
    // "SqsManagedSseEnabled": boolean;
    // KmsMasterKeyId
    // KmsDataKeyReusePeriodSeconds
    "VisibilityTimeout": number;
    "FifoQueue": boolean;
    // "FifoThroughputLimit"?: "perQueue" | "perMessageGroupId";
    "ContentBasedDeduplication": boolean;
    // "DeduplicationScope"?: "messageGroup" | "queue";
  };
}

export const QueuesCollection = new Mongo.Collection<Queue>('Queues');
