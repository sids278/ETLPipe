import { createClient } from '@clickhouse/client';

export const clickhouse = createClient({
  url: process.env.CH_URL ?? 'http://localhost:8123',
  username: process.env.CH_USER ?? 'default',
  password: process.env.CH_PASSWORD ?? '',
  request_timeout: 30000,
});
