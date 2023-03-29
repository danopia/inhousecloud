import { Meteor } from 'meteor/meteor';
import { QueuesCollection } from '/imports/db/queues';
import { TopicsCollection } from '/imports/db/topics';

import './service-apis';

Meteor.publish(null, () => [
  TopicsCollection.find(),
  QueuesCollection.find(),
])
