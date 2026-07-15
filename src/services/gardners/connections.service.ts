import { Readable, Writable } from 'stream';
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
  // Downloads to a local path using the protocol's concurrent/optimized
  // transfer method rather than a plain stream — see the note above
  // downloadToLocalFile in fetcher.service.ts for why this exists.
  downloadToFile(path: string, localPath: string): Promise<void>;
}

interface OpenConnection {
  client: GardnersRemoteClient;
  close(): Promise<void>;
}

// The Biblio ONIX zips (up to ~1.7GB uncompressed) take long enough to
// stream that an idle SSH connection can otherwise be silently dropped
// mid-transfer (observed live: a `read ETIMEDOUT` mid-download that
// ssh2-sftp-client only logs by default, without rejecting anything —
// see biblio.service.ts's use of stream.pipeline for the other half of
// this fix). Keepalive packets prevent the connection from going idle
// enough to trigger that in the first place.
const SFTP_KEEPALIVE_OPTIONS = {
  keepaliveInterval: 10_000,
  keepaliveCountMax: 5,
  readyTimeout: 20_000,
};

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
    async downloadToFile(path, localPath) {
      // fastGet issues many concurrent SFTP READ requests (64 by default)
      // instead of one at a time — createReadStream's plain streaming read
      // is latency-bound (~160KB/s observed live against Gardners' UK
      // servers) since each read waits for the previous one's response;
      // fastGet is bandwidth-bound (~9.7MB/s observed), matching what a
      // plain `sftp get` CLI command achieves.
      await client.fastGet(path, localPath);
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
      // basic-ftp only allows one in-flight command per Client — a fire-
      // and-forget downloadTo() into a live PassThrough (returned before
      // the transfer finishes) races the *next* command issued on this
      // same client, since the caller has no way to know the download is
      // still in flight. That race is real, not theoretical: it surfaced
      // live as "User launched a task while another one is still running"
      // once callers stopped pacing requests with an inter-call delay.
      // Fully awaiting downloadTo() before returning fixes it, but only
      // works if the destination never backpressures — the default
      // highWaterMark PassThrough would otherwise deadlock downloadTo()
      // for anything past ~16KB, since nothing reads from it until this
      // function returns. Sinking into a plain array first (unbounded,
      // never backpressures) avoids that, then handing back a real
      // Readable once the whole transfer — and the client's single
      // command slot — is done. Only used for individually-small files
      // (cover images); large feeds go through downloadToFile instead.
      const chunks: Buffer[] = [];
      const sink = new Writable({
        write(chunk: Buffer, _enc, callback) {
          chunks.push(chunk);
          callback();
        },
      });
      await client.downloadTo(sink, path);
      return Readable.from(Buffer.concat(chunks));
    },
    async size(path) {
      return client.size(path);
    },
    async downloadToFile(path, localPath) {
      await client.downloadTo(localPath, path);
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
    ...SFTP_KEEPALIVE_OPTIONS,
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
    ...SFTP_KEEPALIVE_OPTIONS,
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
