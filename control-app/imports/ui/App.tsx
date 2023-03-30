import React from 'react';
import { QueueListPage } from './QueueListPage';
import { RecentQueueMessagesPage } from './RecentQueueMessagesPage';

export const App = () => (
  <div>
    <h1>In-House Cloud - Control Plane</h1>
    <RecentQueueMessagesPage />
    <QueueListPage />
  </div>
);
