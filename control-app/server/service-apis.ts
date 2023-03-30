import { Meteor } from 'meteor/meteor';
import { WebApp } from 'meteor/webapp';

import { handleStsAction } from './services/sts';
import { handleSnsAction } from './services/sns';
import { handleSqsAction } from './services/sqs';

WebApp.connectHandlers.use('/', (req, res, next) => {
  if (req.method !== 'POST') return next();

  const data = new Array<string>();
  req.setEncoding('utf-8');
  req.on('data', x => data.push(x));
  req.on('end', Meteor.bindEnvironment(async () => {
    const reqBody = data.join('');
    const reqParams = new URLSearchParams(reqBody);
    const reqAuth = new Map(req.headers['authorization']?.slice('AWS4-HMAC-SHA256 '.length).split(', ').map(x => x.split('=') as [string,string]));
    const [accessKeyId, sigDate, region, service, sigVersion] = reqAuth.get('Credential')?.split('/') ?? [];
    if (reqParams.get('Action') !== 'ReceiveMessage') {
      console.log(`${service} API:`, reqParams.get('Action'), reqParams.get('QueueUrl')?.split('/')[4] ?? reqParams);
    }
    const accountId = '123456123456';

    function sendXml(status: number, text: string) {
      res.setHeader('content-type', 'application/xml');
      res.writeHead(status);
      res.end(text);
    }

    try {
      switch (true) {

        case service == 'sts' || reqParams.get('Action')?.startsWith('AssumeRoleWith'):
          return sendXml(200, await handleStsAction(reqParams, accountId, region));

        case service == 'sns':
          return sendXml(200, await handleSnsAction(reqParams, accountId, region));

        case service == 'sqs':
          return sendXml(200, await handleSqsAction(reqParams, accountId, region));

        default: throw new Meteor.Error(`Unimplemented`, `Service ${service} is not available`);
      }

    } catch (err) {
      if (err instanceof Meteor.Error) {
        console.log('Returning error:', err.message);
        sendXml(400, `<ErrorResponse><Error><Type>Sender</Type><Code>${err.error}</Code><Message>${err.reason}</Message></Error></ErrorResponse>`);
      } else {
        const genErr = err as Error;
        console.log('Uncaught error:', genErr.message);
        sendXml(400, `<ErrorResponse><Error><Type>Sender</Type><Code>ServerError</Code><Message>${genErr.message}</Message></Error></ErrorResponse>`);
      }
    }

  }));
});
