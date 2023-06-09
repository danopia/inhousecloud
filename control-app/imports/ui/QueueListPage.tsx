import React from 'react';
import { useTracker } from 'meteor/react-meteor-data';
import { QueuesCollection } from '../db/queues';

export const QueueListPage = () => {
  const queues = useTracker(() => {
    return QueuesCollection.find({}, {sort: {messagesActive: -1, lastPolledAt: -1, name: 1}}).fetch();
  });

  return (
    <div>
      <h2>Queues</h2>
      <table border={1}>
        <thead>
          <tr>
            <th>Queue</th>
            <th>Last Polled</th>
            <th># Active</th>
            <th># Visible</th>
            <th># Delayed</th>
          </tr>
        </thead>
        <tbody>{queues.map(queue => (
          <tr key={queue._id}>
            <td>{queue.name}</td>
            <td>{queue.lastPolledAt?.toLocaleTimeString()}</td>
            <td>{queue.messagesActive}</td>
            <td>{queue.messagesVisible}</td>
            <td>{queue.messagesDelayed}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
};
