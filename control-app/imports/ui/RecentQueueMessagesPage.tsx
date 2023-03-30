import React, { useCallback } from 'react';
import { useTracker } from 'meteor/react-meteor-data';
import { QueueMessage, QueueMessagesCollection } from '../db/queue-messages';

import pako from 'pako';
function grabBody(body: string, compressed: boolean): string {
  if (body?.[0] == '{') {
    const data = JSON.parse(body);
    if (typeof data.Message == 'string') {
      return grabBody(data.Message, compressed) ?? '';
    }
  }
  if (compressed) return decompress(body);
  return body;
}
function decompress(text: string) {
  try {
    const input = decodeBase64(text);
    const output = pako.inflate(input);
    return new TextDecoder().decode(output);
  } catch (err) {
    return err.message;
  }
}
export function decodeBase64(b64: string): Uint8Array {
  const binString = atob(b64);
  const size = binString.length;
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    bytes[i] = binString.charCodeAt(i);
  }
  return bytes;
}

function traceId(msg: QueueMessage) {
  try {
    const datadogPayload = msg.attributes?.['_datadog']?.value;
    if (!datadogPayload) return '';
    const textPayload = datadogPayload instanceof Uint8Array
      ? new TextDecoder().decode(datadogPayload)
      : datadogPayload;
    return JSON.parse(textPayload)['x-datadog-trace-id']
  } catch (err) {
    return err.message;
  }
}

export const RecentQueueMessagesPage = () => {
  const messages = useTracker(() => {
    return QueueMessagesCollection.find({}, {sort: {createdAt: -1}}).fetch();
  });

  return (
    <div>
      <h2>Queue Messages</h2>
      <table border={1}>
        <thead>
          <tr>
            <th>Queue</th>
            <th>Lifecycle</th>
            <th>Sent at</th>
            <th>Last delivered</th>
            <th>Deliver count</th>
            <th>Datadog Trace ID</th>
            <th>Body size</th>
            <th>Compressed</th>
          </tr>
        </thead>
        <tbody>{messages.map(msg => (
          <tr key={msg._id} style={msg.lifecycle == 'Deleted' ? {color: '#999'} : {}}>
            <td>{msg.queueId.split(':')[5]}</td>
            <td>{msg.lifecycle}</td>
            <td>{msg.createdAt?.toLocaleTimeString()}</td>
            <td>{msg.lastDeliveredAt?.toLocaleTimeString()}</td>
            <td>{msg.totalDeliveries}</td>
            <td>{traceId(msg)}</td>
            <td>{msg.body.length}</td>
            <td>{msg.attributes?.['compression']?.value}</td>
            <td><button onClick={() => navigator.clipboard.writeText(grabBody(msg.body, msg.attributes?.['compression']?.value == 'true'))}>Copy</button></td>
            <td>{grabBody(msg.body, msg.attributes?.['compression']?.value == 'true').slice(0, 64)}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
};
