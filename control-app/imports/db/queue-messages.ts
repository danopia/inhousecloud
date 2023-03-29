import { Meteor } from 'meteor/meteor';
import { Mongo } from 'meteor/mongo';
import { Queue } from './queues';

export type QueueMessageAttribute = {
  dataType: 'string';
  value: string;
};

export interface QueueMessage {
  _id: string;
  queueId: string;
  createdAt: Date;
  modifiedAt: Date;

  lifecycle: 'Waiting' | 'Delivered' | 'Deleted';
  firstDeliveredAt?: Date;
  lastDeliveredAt?: Date;
  deletedAt?: Date;
  visibleAfter?: Date;
  totalDeliveries: number;

  body: string;
  delaySeconds: number;
  groupId?: string;
  dedupId?: string;
  attributes: Record<string, QueueMessageAttribute>;
  systemAttributes: Record<string, QueueMessageAttribute>;
}

export const QueueMessagesCollection = new Mongo.Collection<QueueMessage>('QueueMessages');

export async function sendQueueMessage(
  queue: Queue,
  message: {
    body?: string | null;
    dedupId?: string | null;
    groupId?: string | null;
  } & Partial<Pick<QueueMessage,
    | 'delaySeconds'
    | 'attributes'
    | 'systemAttributes'
  >>,
) {
  if (!message.body) throw new Meteor.Error(`no-body`, `body is required`);

  if (queue.config.FifoQueue) {
    if (!message.groupId) throw new Meteor.Error(`fifo`,
      `This is a fifo queue`);
    if (queue.config.ContentBasedDeduplication && !message.dedupId) throw new Meteor.Error(`fifo`,
      `This fifo queue requires a MessageDeduplicationId because ContentBasedDeduplication is not set`);
  } else {
    if (message.groupId || message.dedupId) throw new Meteor.Error(`fifo`, `Is not a fifo queue`);
  }

  // TODO: check that systemAttrs only has AWSTraceHeader if anything

  const messageId = await insertQueueMessage({
    queueId: queue._id,

    body: message.body,
    dedupId: message.dedupId ?? undefined,
    groupId: message.groupId ?? undefined,

    delaySeconds: message.delaySeconds ?? 0,
    attributes: message.attributes ?? {},
    systemAttributes: message.systemAttributes ?? {},
  });

  return {
    messageId,
  };
}

export async function insertQueueMessage(
  opts: Pick<QueueMessage,
    | 'queueId' | 'dedupId' | 'groupId' | 'body' | 'delaySeconds' | 'attributes' | 'systemAttributes'
  >,
) {
  return await QueueMessagesCollection.insertAsync({
    ...opts,
    createdAt: new Date,
    modifiedAt: new Date,
    totalDeliveries: 0,

    lifecycle: 'Waiting',
    visibleAfter: opts.delaySeconds
      ? new Date(Date.now() + (opts.delaySeconds * 1000))
      : new Date(0),
  });
}

export async function receiveQueueMessages(
  queue: Queue,
  maxMessages: number,
) {
  const availMessages = await QueueMessagesCollection.find({
    queueId: queue._id,
    visibleAfter: {$lt: new Date()},
  }, {
    sort: { visibleAfter: 1 },
    limit: Math.min(maxMessages, 10),
  }).fetchAsync();

  const delivarables: Array<QueueMessage> = [];
  // update in loop because lack of https://feedback.mongodb.com/forums/924280-database/suggestions/46072024-how-to-limit-the-number-of-document-updates
  for (const msg of availMessages) {
    console.log('Considering delivering', msg._id);
    if (await QueueMessagesCollection.updateAsync({
      _id: msg._id,
      lifecycle: msg.lifecycle,
      lastDeliveredAt: msg.lastDeliveredAt,
    }, {
      $set: {
        lifecycle: 'Delivered',
        firstDeliveredAt: msg.firstDeliveredAt ?? new Date,
        lastDeliveredAt: new Date,
        visibleAfter: new Date(Date.now() + (queue.config.VisibilityTimeout*1000)),
      },
      $inc: {
        totalDeliveries: 1,
      },
    })) {
      delivarables.push(msg);
    }
  }
  return delivarables;
}

export async function deleteQueueMessage(queue: Queue, handle: string) {
  const [msgId, receives] = handle.split('/');

  const hit = await QueueMessagesCollection.updateAsync({
    _id: msgId,
    queueId: queue._id,
    totalDeliveries: parseInt(receives)+1,
    lifecycle: 'Delivered',
  }, {
    $set: {
      lifecycle: 'Deleted',
      deletedAt: new Date,
      modifiedAt: new Date,
    },
    $unset: {
      visibleAfter: 1,
    },
  });

  if (!hit) throw new Meteor.Error(`ReceiptHandleIsInvalid`,
    `Failed to find deletable message.`);
}
