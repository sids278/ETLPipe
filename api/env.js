import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';

// api/.env holds API settings; the repo-root .env holds the passwords shared with docker compose and ch-sync.
// Neither overrides a variable that is already set, and api/.env wins over the root file.
dotenv.config({
  path: [
    fileURLToPath(new URL('./.env', import.meta.url)),
    fileURLToPath(new URL('../.env', import.meta.url)),
  ],
  quiet: true,
});
