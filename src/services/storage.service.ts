import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { Readable } from 'stream';
import { config } from '../config';

class StorageService {
  private readonly s3: S3Client;
  private readonly bucket: string;

  constructor() {
    this.s3 = new S3Client({
      region: 'auto',
      endpoint: config.r2.endpoint,
      credentials: {
        accessKeyId: config.r2.accessKeyId,
        secretAccessKey: config.r2.secretAccessKey,
      },
    });
    this.bucket = config.r2.bucketName;
  }

  async getFileStream(fileKey: string): Promise<Readable> {
    const command = new GetObjectCommand({ Bucket: this.bucket, Key: fileKey });
    const response = await this.s3.send(command);

    if (!response.Body) {
      throw new Error(`Empty response body for key: ${fileKey}`);
    }

    return response.Body as Readable;
  }

  async listOnixFiles(): Promise<{ key: string; lastModified: Date; size: number }[]> {
    const results: { key: string; lastModified: Date; size: number }[] = [];
    let continuationToken: string | undefined;

    do {
      const command = new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: config.r2.onixPrefix,
        ContinuationToken: continuationToken,
      });

      const response = await this.s3.send(command);

      for (const obj of response.Contents ?? []) {
        if (obj.Key && obj.Key.endsWith('.xml')) {
          results.push({
            key: obj.Key,
            lastModified: obj.LastModified ?? new Date(0),
            size: obj.Size ?? 0,
          });
        }
      }

      continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    return results;
  }

  /**
   * Returns a presigned PUT URL valid for `expiresIn` seconds.
   * The caller uses it to upload directly to R2 — the file never touches this server.
   */
  async getPresignedUploadUrl(
    fileKey: string,
    expiresIn = 4 * 60 * 60, // 4 hours — enough for a slow 27 GB upload
  ): Promise<string> {
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: fileKey,
      ContentType: 'application/xml',
    });
    return getSignedUrl(this.s3, command, { expiresIn });
  }

  async fileExists(fileKey: string): Promise<boolean> {
    try {
      await this.s3.send(new HeadObjectCommand({ Bucket: this.bucket, Key: fileKey }));
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Streams a Readable directly into R2 without buffering it fully in memory.
   * Used for large files fetched from Gardners SFTP/FTP (e.g. the ONIX Biblio
   * zip entry). `PutObjectCommand` requires a known Content-Length when the
   * SDK can't determine one from the stream itself — pass `contentLength`
   * (from an SFTP `stat()`/FTP `size()` call) for large/unknown-length
   * streams; without it, R2 may reject or buffer the upload internally.
   */
  async uploadStream(
    key: string,
    stream: Readable,
    contentType: string,
    contentLength?: number,
  ): Promise<void> {
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: key,
      Body: stream,
      ContentType: contentType,
      ContentLength: contentLength,
    });
    await this.s3.send(command);
  }

  /**
   * Uploads a raw buffer to R2 and returns the R2 key.
   * Use this for cover images fetched from external sources.
   */
  async uploadBuffer(key: string, buffer: Buffer, contentType: string): Promise<void> {
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: key,
      Body: buffer,
      ContentType: contentType,
    });
    await this.s3.send(command);
  }

  /**
   * Serialises a value to JSON and uploads it to R2.
   * Used to store parsed ONIX chunk payloads out of PostgreSQL.
   */
  async uploadJson(key: string, value: unknown): Promise<void> {
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: key,
      Body: JSON.stringify(value),
      ContentType: 'application/json',
    });
    await this.s3.send(command);
  }

  /**
   * Downloads and parses a JSON object previously written by uploadJson.
   */
  async getJson<T>(key: string): Promise<T> {
    const command = new GetObjectCommand({ Bucket: this.bucket, Key: key });
    const response = await this.s3.send(command);

    if (!response.Body) {
      throw new Error(`Empty response body for key: ${key}`);
    }

    const text = await response.Body.transformToString('utf-8');
    return JSON.parse(text) as T;
  }

  /**
   * Deletes a single R2 object. Non-fatal if the key doesn't exist.
   */
  async deleteObject(key: string): Promise<void> {
    await this.s3.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key }));
  }
}

export const storageService = new StorageService();
