import { Meteor } from "meteor/meteor";

export async function handleStsAction(reqParams: URLSearchParams, accountId: string, region: string) {
  switch (reqParams.get('Action')) {

  case 'GetCallerIdentity':
    return `<Result><GetCallerIdentityResult>
      <UserId>UserId</UserId>
      <Account>${accountId}</Account>
      <Arn>Arn</Arn>
    </GetCallerIdentityResult></Result>`;

  case 'AssumeRoleWithWebIdentity':
    const RoleArn = reqParams.get('RoleArn');
    const RoleSessionName = reqParams.get('RoleSessionName');
    const WebIdentityToken = reqParams.get('WebIdentityToken');

    // throw new Meteor.Error(`TODO`, `TODO: parse OIDC JWT`);

    const oidcToken: {
      "exp": number;
      "iat": number;
      "nbf": number;
      "iss": string; // "https://container.googleapis.com/v1/projects/my-project/locations/europe-west1/clusters/my-gke"
      "aud": Array<string>;
      "sub": string; // "system:serviceaccount:my-namespace:my-sa"
      "kubernetes.io"?: {
        "namespace": string;
        "pod": {
          "name": string;
          "uid": string;
        };
        "serviceaccount": {
          "name": string;
          "uid": string;
        };
      };
    } = JSON.parse(Buffer.from(WebIdentityToken!.split('.')[1], 'base64url').toString('utf-8'));

    if (oidcToken["kubernetes.io"]) {
      const kubeData = oidcToken["kubernetes.io"];
      console.log(`Received kubernetes OIDC token from ${kubeData.namespace}/${kubeData.pod.name}`);
      return `<Result>
      <AssumeRoleWithWebIdentityResult>
        <SubjectFromWebIdentityToken>${oidcToken.sub}</SubjectFromWebIdentityToken>
        <Audience>${oidcToken.aud}</Audience>
        <AssumedRoleUser>
          <Arn>arn:aws:sts::123456123456:assumed-role/${kubeData.namespace}/${kubeData.serviceaccount.name}</Arn>
          <AssumedRoleId>${kubeData.namespace}:${kubeData.serviceaccount.uid}</AssumedRoleId>
        </AssumedRoleUser>
        <Credentials>
          <AccessKeyId>ASgeIAIOSFODNN7EXAMPLE</AccessKeyId>
          <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>
          <SessionToken>-------------------------</SessionToken>
          <Expiration>${new Date(Date.now() + 15 * 60 * 1000).toISOString()}</Expiration>
        </Credentials>
        <SourceIdentity>k8s/${kubeData.namespace}/${kubeData.pod.name}</SourceIdentity>
        <Provider>www.amazon.com</Provider>
      </AssumeRoleWithWebIdentityResult></Result>`;
    }
    throw new Meteor.Error(`InvalidIdentityToken`, `This is not a Kubernetes token!`);

  default:
    throw new Meteor.Error(`Unimplemented`, `Unimplemented`);
  }
}
