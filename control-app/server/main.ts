import { Meteor } from 'meteor/meteor';
import { QueuesCollection } from '/imports/db/queues';
import { TopicsCollection } from '/imports/db/topics';

import './service-apis';
import { QueueMessagesCollection } from '/imports/db/queue-messages';

Meteor.publish(null, () => [
  TopicsCollection.find(),
  QueuesCollection.find(),
  QueueMessagesCollection.find({}, {sort: {modifiedAt: -1}, limit: 50}),
])
