import { Meteor } from "meteor/meteor";

export function getPrefixedParams(params: URLSearchParams, prefix: string) {
  return new URLSearchParams(Array.from(params)
    .flatMap(pair => pair[0].startsWith(prefix)
      ? [[pair[0].slice(prefix.length), pair[1]]]
      : []));
}

export function extractParamArray(reqParams: URLSearchParams, prefix: string, suffix: string) {
  const entries: Array<URLSearchParams> = [];
  for (let i = 1; reqParams.has(`${prefix}${i}${suffix}`); i++) {
    const params = getPrefixedParams(reqParams, `${prefix}${i}.`);
    entries.push(params);
  }
  return entries;
}

export const extractMessageAttributes = (reqParams: URLSearchParams, prefix: string) => Object
  .fromEntries(extractParamArray(reqParams, prefix, '.Name')
    .map(params => {
      const name = params.get(`Name`)!;
      const dataType = params.get(`Value.DataType`);
      switch (dataType) {
        case 'String':
          return [name, {
            dataType: 'String' as const,
            value: params.get(`Value.StringValue`)!,
          }];
        case 'Binary':
          return [name, {
            dataType: 'Binary' as const,
            value: Buffer.from(params.get(`Value.BinaryValue`)!, 'base64'),
          }];
        default: throw new Meteor.Error(`unimpl`, `TODO: attribute data type ${dataType}`);
      }
    }));
