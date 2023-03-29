import { Meteor } from 'meteor/meteor';
import { Mongo } from 'meteor/mongo';
import { TopicSubscriptionsCollection } from './topic-subscriptions';
import { Topic } from './topics';

export type TopicMessageAttribute = {
  dataType: 'string';
  value: string;
};

export interface TopicMessage {
  _id: string;
  topicId: string;
  createdAt: Date;
  modifiedAt: Date;

  undeliveredTo: Array<string>;
  deliveredTo: Array<string>;
  lastDeliveredAt?: Date;

  subject?: string;
  body: string;
  groupId?: string;
  dedupId?: string;
  attributes: Record<string, TopicMessageAttribute>;
  messageStructure?: 'json';
  // PhoneNumber, Subject, TargetArn,
}

export const TopicMessagesCollection = new Mongo.Collection<TopicMessage>('TopicMessages');

export async function sendTopicMessage(
  topic: Topic,
  message: {
    subject?: string | null;
    body?: string | null;
    dedupId?: string | null;
    groupId?: string | null;
    messageStructure?: string | null;
  } & Partial<Pick<TopicMessage,
    | 'attributes'
  >>,
) {
  if (!message.body) throw new Meteor.Error(`no-body`, `body is required`);

  if (topic.config.FifoTopic) {
    if (!message.groupId) throw new Meteor.Error(`fifo`,
      `This is a fifo Topic`);
    if (topic.config.ContentBasedDeduplication && !message.dedupId) throw new Meteor.Error(`fifo`,
      `This fifo Topic requires a MessageDeduplicationId because ContentBasedDeduplication is not set`);
  } else {
    if (message.groupId || message.dedupId) throw new Meteor.Error(`fifo`, `Is not a fifo Topic`);
  }

  const messageId = await insertTopicMessage({
    topicId: topic._id,

    subject: message.subject ?? undefined,
    body: message.body,
    dedupId: message.dedupId ?? undefined,
    groupId: message.groupId ?? undefined,
    attributes: message.attributes ?? {},
  });

  return {
    messageId,
  };
}

export async function insertTopicMessage(
  opts: Pick<TopicMessage,
    | 'topicId' | 'dedupId' | 'groupId' | 'subject' | 'body' | 'attributes' | 'messageStructure'
  >,
) {
  return await TopicMessagesCollection.insertAsync({
    ...opts,
    createdAt: new Date,
    modifiedAt: new Date,

    undeliveredTo: await TopicSubscriptionsCollection.find({topicId: opts.topicId}).mapAsync(x => x._id),
    deliveredTo: [],
  });
}
