import { Meteor } from "meteor/meteor";
import { Random } from "meteor/random";
import { sendQueueMessage } from "/imports/db/queue-messages";
import { QueuesCollection } from "/imports/db/queues";
import { TopicSubscriptionsCollection } from "../../imports/db/topic-subscriptions";
import { sendTopicMessage, TopicMessage, TopicMessagesCollection } from "/imports/db/topic-messages";
import { TopicsCollection } from "/imports/db/topics";
import { extractMessageAttributes, extractParamArray } from "/imports/params";

export async function handleSnsAction(reqParams: URLSearchParams, accountId: string, region: string): Promise<string> {
  switch (reqParams.get('Action')) {

  case 'CreateTopic':
    const arn = `arn:aws:sns:${region}:${accountId}:${reqParams.get('Name')}`;
    const attributes: Record<string,string> = {};
    for (let i = 1; reqParams.has(`Attributes.entry.${i}.key`); i++) {
      attributes[reqParams.get(`Attributes.entry.${i}.key`)!] = reqParams.get(`Attributes.entry.${i}.value`)!;
    }
    const tags: Record<string,string> = {};
    for (let i = 1; reqParams.has(`Tags.member.${i}.Key`); i++) {
      tags[reqParams.get(`Tags.member.${i}.Key`)!] = reqParams.get(`Tags.member.${i}.Value`)!;
    }

    const isNamedFifo = arn.endsWith('.fifo');
    if (isNamedFifo !== (attributes['FifoTopic'] == 'true')) throw new Meteor.Error(`InvalidFifoTopic`,
      `Is "${reqParams.get('Name')}" fifo? You said "${attributes['FifoTopic']}"`);

    await TopicsCollection.insertAsync({
      _id: arn,
      region,
      accountId,
      createdAt: new Date,
      modifiedAt: new Date,
      name: reqParams.get('Name')!,
      // attributes: attributes,
      tags: tags,
      config: {
        DisplayName: attributes['DisplayName'] || "",
        Policy: attributes['Policy'] || '{"Version":"2012-10-17"}',
        EffectiveDeliveryPolicy: attributes['EffectiveDeliveryPolicy'] || '{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false}}',
        LambdaSuccessFeedbackSampleRate: parseInt(attributes['LambdaSuccessFeedbackSampleRate'] || '0'),
        FirehoseSuccessFeedbackSampleRate: parseInt(attributes['FirehoseSuccessFeedbackSampleRate'] || '0'),
        SQSSuccessFeedbackSampleRate: parseInt(attributes['SQSSuccessFeedbackSampleRate'] || '0'),
        HTTPSuccessFeedbackSampleRate: parseInt(attributes['HTTPSuccessFeedbackSampleRate'] || '0'),
        ApplicationSuccessFeedbackSampleRate: parseInt(attributes['ApplicationSuccessFeedbackSampleRate'] || '0'),
        FifoTopic: attributes['FifoTopic'] == 'true',
        ContentBasedDeduplication: attributes['ContentBasedDeduplication'] == 'true',
      },
    });
    return `<Result><CreateTopicResult><TopicArn>${arn}</TopicArn></CreateTopicResult></Result>`;

  case 'SetTopicAttributes':
    const attrName = reqParams.get('AttributeName')!;
    const attrValue = reqParams.get('AttributeValue');

    const change: Record<string,string> = {};
    if (['Policy', 'EffectiveDeliveryPolicy', 'DisplayName'].includes(attrName)) {
      change[`config.${attrName}`] = attrValue!;
    } else if (['LambdaSuccessFeedbackSampleRate', 'FirehoseSuccessFeedbackSampleRate', 'SQSSuccessFeedbackSampleRate', 'HTTPSuccessFeedbackSampleRate', 'ApplicationSuccessFeedbackSampleRate', 'ContentBasedDeduplication'].includes(attrName)) {
      change[`config.${attrName}`] = `${attrValue}`;
    } else throw new Meteor.Error(`unimpl`, `Can't set ${attrName} on topic`);

    const matched = await TopicsCollection.updateAsync({
      _id: reqParams.get('TopicArn')!,
    }, {
      // TODO: unset if no value
      $set: change,
    });
    if (matched < 1) throw new Meteor.Error(404, 'no-topic');
    return `<Result />`;

  case 'GetTopicAttributes':
    const latest = await TopicsCollection.findOneAsync({
      _id: reqParams.get('TopicArn')!,
    });
    if (!latest) throw new Meteor.Error(404, 'no-topic');

    const allAttrs = {
      "TopicArn": latest._id,
      "Owner": `${latest.accountId}`,

      "SubscriptionsPending": "0",
      "SubscriptionsConfirmed": "0",
      "SubscriptionsDeleted": "0",

      "DisplayName": latest.config.DisplayName,
      "Policy": latest.config.Policy,
      "EffectiveDeliveryPolicy": latest.config.EffectiveDeliveryPolicy,
      "FifoTopic": latest.config.FifoTopic ? 'true' : 'false',
      "ContentBasedDeduplication": latest.config.ContentBasedDeduplication ? 'true' : 'false',
      "LambdaSuccessFeedbackSampleRate": `${latest.config.LambdaSuccessFeedbackSampleRate}`,
      "FirehoseSuccessFeedbackSampleRate": `${latest.config.FirehoseSuccessFeedbackSampleRate}`,
      "SQSSuccessFeedbackSampleRate": `${latest.config.SQSSuccessFeedbackSampleRate}`,
      "HTTPSuccessFeedbackSampleRate": `${latest.config.HTTPSuccessFeedbackSampleRate}`,
      "ApplicationSuccessFeedbackSampleRate": `${latest.config.ApplicationSuccessFeedbackSampleRate}`,
    };

    return `<Result><GetTopicAttributesResult>        <Attributes>
    ${Object.entries(allAttrs).map(pair => `<entry>
      <key>${pair[0]}</key>
      <value>${pair[1]}</value>
    </entry>`).join('\n')}</Attributes></GetTopicAttributesResult></Result>`;

  case 'ListTagsForResource':
    const latest2 = await TopicsCollection.findOneAsync({
      _id: reqParams.get('ResourceArn')!,
    });
    if (!latest2) throw new Meteor.Error(404, 'no-topic');
    return `<Result><ListTagsForResourceResult>        <Tags>
    ${Object.entries(latest2.tags).map(pair => `<member>
      <Key>${pair[0]}</Key>
      <Value>${pair[1]}</Value>
    </member>`).join('\n')}</Tags></ListTagsForResourceResult></Result>`;

  case 'DeleteTopic':
    const happened = await TopicsCollection.removeAsync({
      _id: reqParams.get('TopicArn')!,
    });
    if (!happened) throw new Meteor.Error(404, 'no-topic');
    return `<Result><DeleteTopicResult /></Result>`;

  case 'Subscribe':
    const topic = await TopicsCollection.findOneAsync({_id: reqParams.get('TopicArn')!});
    if (!topic) throw new Meteor.Error(404, 'no-topic');
    if (reqParams.get('Protocol') !== 'sqs') throw new Meteor.Error(`unimpl`,
      `Protocol ${reqParams.get('Protocol')} not implemented`);
    const queue = await QueuesCollection.findOneAsync({_id: reqParams.get('Endpoint')!});
    if (!queue) throw new Meteor.Error(404, 'no-queue');
    const subId = await TopicSubscriptionsCollection.insertAsync({
      _id: `${topic._id}:${Random.id()}`,
      topicId: topic._id,
      accountId,
      createdAt: new Date,
      modifiedAt: new Date,
      endpoint: {
        protocol: 'sqs',
        queueId: queue._id,
      },
      pendingConfirmation: false,
      confirmationWasAuthenticated: true,
      config: {
        // TODO
        RawMessageDelivery: false,
      },
    });
    return `<Result><SubscribeResult><SubscriptionArn>${subId}</SubscriptionArn></SubscribeResult></Result>`;

  case 'GetSubscriptionAttributes': {
    const sub = await TopicSubscriptionsCollection.findOneAsync({_id: reqParams.get('SubscriptionArn')!});
    if (!sub) throw new Meteor.Error(404, 'no-sub');
    const allAttrs = {
      "Owner": sub.accountId,
      "RawMessageDelivery": `${sub.config.RawMessageDelivery}`,
      "TopicArn": sub.topicId,
      "Endpoint": sub.endpoint.queueId,
      "Protocol": sub.endpoint.protocol,
      "PendingConfirmation": `${sub.pendingConfirmation}`,
      "ConfirmationWasAuthenticated": `${sub.confirmationWasAuthenticated}`,
      "SubscriptionArn": sub._id,
    };

    return `<Result><GetSubscriptionAttributesResult>        <Attributes>
    ${Object.entries(allAttrs).map(pair => `<entry>
      <key>${pair[0]}</key>
      <value>${pair[1]}</value>
    </entry>`).join('\n')}</Attributes></GetSubscriptionAttributesResult></Result>`;
  }

  case 'ListTopics': {
    const topics = await TopicsCollection.find({
      region, accountId,
    }, {
      sort: {name: 1},
    }).fetchAsync();
    return `<Result><ListTopicsResult><Topics>${topics.map(x => `
      <member><TopicArn>${x._id}</TopicArn></member>`
    ).join('\n')}
    </Topics></ListTopicsResult></Result>`;
  }




  // data plane

  case 'Publish': {
    const topic = await TopicsCollection.findOneAsync({
      _id: reqParams.get('TopicArn')!,
    });
    if (!topic) throw new Meteor.Error(404, 'no-topic');

    const { messageId } = await sendTopicMessage(topic, {
      body: reqParams.get('Message'),
      dedupId: reqParams.get('MessageDeduplicationId'),
      groupId: reqParams.get('MessageGroupId'),
      messageStructure: reqParams.get('MessageStructure'),
      attributes: extractMessageAttributes(reqParams, 'MessageAttribute.'),
    });

    return `<Response><PublishResult><MessageId>${messageId}</MessageId></PublishResult></Response>`;
  }

  case 'PublishBatch': {
    const topic = await TopicsCollection.findOneAsync({
      _id: reqParams.get('TopicArn')!,
    });
    if (!topic) throw new Meteor.Error(404, 'no-topic');

    const Successful = new Array<string>;
    const Failed = new Array<string>;
    for (const params of extractParamArray(reqParams, 'PublishBatchRequestEntries.member.', '.Id')) {
      const msgId = params.get('Id');
      try {
        const { messageId } = await sendTopicMessage(topic, {
          body: params.get('Message'),
          dedupId: params.get('MessageDeduplicationId'),
          groupId: params.get('MessageGroupId'),
          messageStructure: params.get('MessageStructure'),
          attributes: extractMessageAttributes(params, 'MessageAttribute.'),
        });
        Successful.push(`
          <member>
            <Id>${msgId}</Id>
            <MessageId>${messageId}</MessageId>
            <MD5OfMessageBody>0e024d309850c78cba5eabbeff7cae71</MD5OfMessageBody>
          </member>`);
      } catch (err) {
        if (!(err instanceof Meteor.Error)) throw err;
        Failed.push(`
          <member>
            <Id>${msgId}</Id>
            <Code>${err.error}</Code>
            <Message>${err.reason}</Message>
            <SenderFault>true</SenderFault>
            <MD5OfMessageBody>0e024d309850c78cba5eabbeff7cae71</MD5OfMessageBody>
          </member>`);
      }
    }

    return `
    <Response><PublishBatchResult>
      <Successful>${Successful.join('')}</Successful>
      <Failed>${Failed.join('')}</Failed>
    </PublishBatchResult></Response>`;
  }



  default:
    throw new Meteor.Error(`Unimplemented`, `Unimplemented`);
  }
}

Meteor.setInterval(async () => {
  // TODO: this double delivers if servers overlap timers
  const msgs = await TopicMessagesCollection.find({
    undeliveredTo: { $exists: true, $ne: [] as any },
  }, {
    sort: { createdAt: -1 },
    limit: 10,
  }).fetchAsync();

  for (const msg of msgs) {
    const deliverResults = await Promise.allSettled(msg.undeliveredTo.map(async subId => {
      const sub = await TopicSubscriptionsCollection.findOneAsync({ _id: subId });
      if (!sub) throw new Meteor.Error('sub-not-found', `Sub doesn't exist`);


      switch (sub.endpoint.protocol) {
        case 'sqs':
          const queue = await QueuesCollection.findOneAsync({ _id: sub.endpoint.queueId });
          if (!queue) throw new Meteor.Error('queue-not-found', `Queue doesn't exist`);
          await sendQueueMessage(queue, {
            attributes: msg.attributes,
            dedupId: msg.dedupId,
            groupId: msg.groupId,
            body: sub.config.RawMessageDelivery ? msg.body : jsonifyMessage(msg),
          });
          // is this racy...?
          await TopicMessagesCollection.updateAsync({
            _id: msg._id,
          }, {
            $set: { modifiedAt: new Date },
            $pull: { undeliveredTo: subId },
            $push: { deliveredTo: subId },
          });
          return subId;

        default: throw new Meteor.Error('todo', `TODO: endpoint type ${sub.endpoint.protocol}`);
      }
    }));

    for (const result of deliverResults) {
      if (result.status == 'fulfilled') {
        console.log('Delivered', msg._id, 'to', result.value);
      } else {
        console.warn(`Failed to deliver`, msg._id, ':', (result.reason as Error).message);
      }
    }

    if (deliverResults.every(x => x.status == 'fulfilled')) {
      await TopicMessagesCollection.updateAsync({
        _id: msg._id,
      }, {
        $set: {
          modifiedAt: new Date,
          lastDeliveredAt: new Date,
        },
      });
    }
  }
}, 5000);

function jsonifyMessage(msg: TopicMessage) {
  return JSON.stringify({
    "Type": "Notification",
    "MessageId": msg._id,
    "TopicArn": msg.topicId,
    "Subject": msg.subject,
    "Message": msg.body,
    "Timestamp": msg.createdAt.toISOString(),
    // "SignatureVersion": "1",
    // "Signature": "LsDr0I...ak4ZDpg8dXg==",
    // "SigningCertURL": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem",
    // "UnsubscribeURL": `https://sns.TODO.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=${sub._id}`,
  });
}
