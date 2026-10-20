import { HumChat } from '@humchat/node';

export function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

export function client() {
  return new HumChat({
    baseUrl: required('HUMCHAT_BASE_URL'),
    orgId: required('HUMCHAT_ORG_ID'),
    tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }),
  });
}
