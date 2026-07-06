import { Readable, PassThrough } from 'stream';
import SftpClient from 'ssh2-sftp-client';
import { Client as FtpClient } from 'basic-ftp';
import { config } from '../../config';

export interface RemoteFileDescriptor {
  path: string;
  filename: string;
  modifiedAt: Date | null;
  size: number;
}

/**
 * Minimal protocol-agnostic surface that feed code is written against — SFTP
 * (ssh2-sftp-client) and plain FTP (basic-ftp) have different native APIs,
 * but every Gardners feed only ever needs to list a directory, check a path
 * exists (e.g. a `.DONE` sentinel), and stream a file. Keeping feed code
 * against this interface means it never has to know which protocol/host a
 * given feed lives on.
 */
export interface GardnersRemoteClient {
  list(dirPath: string): Promise<RemoteFileDescriptor[]>;
  exists(path: string): Promise<boolean>;
  readStream(path: string): Promise<Readable>;
  size(path: string): Promise<number>;
}

interface OpenConnection {
  client: GardnersRemoteClient;
  close(): Promise<void>;
}

function sftpClientAdapter(client: SftpClient): GardnersRemoteClient {
  return {
    async list(dirPath) {
      const entries = await client.list(dirPath);
      return entries
        .filter((e) => e.type === '-')
        .map((e) => ({
          path: `${dirPath.replace(/\/$/, '')}/${e.name}`,
          filename: e.name,
          modifiedAt: e.modifyTime ? new Date(e.modifyTime) : null,
          size: e.size,
        }));
    },
    async exists(path) {
      const result = await client.exists(path);
      return result !== false;
    },
    async readStream(path) {
      return client.createReadStream(path);
    },
    async size(path) {
      const stat = await client.stat(path);
      return stat.size;
    },
  };
}

function ftpClientAdapter(client: FtpClient): GardnersRemoteClient {
  return {
    async list(dirPath) {
      const entries = await client.list(dirPath);
      return entries
        .filter((e) => e.isFile)
        .map((e) => ({
          path: `${dirPath.replace(/\/$/, '')}/${e.name}`,
          filename: e.name,
          modifiedAt: e.modifiedAt ?? null,
          size: e.size,
        }));
    },
    async exists(path) {
      try {
        await client.size(path);
        return true;
      } catch {
        return false;
      }
    },
    async readStream(path) {
      // basic-ftp's downloadTo only accepts a Writable destination — bridge
      // it into a Readable via PassThrough so callers get a stream to pipe
      // from, same shape as the SFTP adapter's createReadStream.
      const passthrough = new PassThrough();
      client.downloadTo(passthrough, path).catch((err) => passthrough.destroy(err));
      return passthrough;
    },
    async size(path) {
      return client.size(path);
    },
  };
}

async function openBespokeSftp(): Promise<OpenConnection> {
  const client = new SftpClient();
  await client.connect({
    host: config.gardners.bespokeSftp.host,
    port: config.gardners.bespokeSftp.port,
    username: config.gardners.bespokeSftp.username,
    password: config.gardners.bespokeSftp.password,
  });
  return { client: sftpClientAdapter(client), close: () => client.end().then(() => undefined) };
}

async function openGenericSftp(): Promise<OpenConnection> {
  const client = new SftpClient();
  await client.connect({
    host: config.gardners.genericSftp.host,
    port: config.gardners.genericSftp.port,
    username: config.gardners.genericSftp.username,
    password: config.gardners.genericSftp.password,
  });
  return { client: sftpClientAdapter(client), close: () => client.end().then(() => undefined) };
}

async function openCoversFtp(): Promise<OpenConnection> {
  const client = new FtpClient();
  await client.access({
    host: config.gardners.coversFtp.host,
    port: config.gardners.coversFtp.port,
    user: config.gardners.coversFtp.username,
    password: config.gardners.coversFtp.password,
  });
  return { client: ftpClientAdapter(client), close: () => Promise.resolve(client.close()) };
}

async function withOpenConnection<T>(
  open: () => Promise<OpenConnection>,
  fn: (client: GardnersRemoteClient) => Promise<T>,
): Promise<T> {
  const conn = await open();
  try {
    return await fn(conn.client);
  } finally {
    await conn.close().catch(() => undefined);
  }
}

const withBespokeSftp = <T>(fn: (client: GardnersRemoteClient) => Promise<T>) =>
  withOpenConnection(openBespokeSftp, fn);
const withGenericSftp = <T>(fn: (client: GardnersRemoteClient) => Promise<T>) =>
  withOpenConnection(openGenericSftp, fn);
const withCoversFtp = <T>(fn: (client: GardnersRemoteClient) => Promise<T>) =>
  withOpenConnection(openCoversFtp, fn);

export const gardnersConnections = {
  // Callback-scoped — connection is closed automatically once fn resolves.
  // Use for list/exists calls and any download that completes within fn.
  withBespokeSftp,
  withGenericSftp,
  withCoversFtp,
  // Caller-managed — for streaming downloads where the Readable must stay
  // open after this function returns (see fetcher.service.ts's
  // downloadToStream). The caller is responsible for closing the connection
  // once the stream ends or errors.
  openBespokeSftp,
  openGenericSftp,
  openCoversFtp,
};

export type GardnersConnectionName = 'bespokeSftp' | 'genericSftp' | 'coversFtp';
