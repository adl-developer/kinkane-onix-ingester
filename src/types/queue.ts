import { GardnersFeed, GardnersConnectionName, RemoteFileDescriptor } from '../services/gardners/fetcher.service';

export interface FileJobData {
  ingestionJobId: number;
  fileKey: string;
}

export interface ChunkJobData {
  ingestionJobId: number;
  chunkId: number;
  chunkIndex: number;
}

export type FileJobResult = {
  totalChunks: number;
  totalBooks: number;
};

export type ChunkJobResult = {
  processedBooks: number;
  failedBooks: number;
};

export interface GardnersFileJobData {
  feed: GardnersFeed;
  connection: GardnersConnectionName;
  file: RemoteFileDescriptor;
  logId: number;
}

export type GardnersFileJobResult = {
  totalChunks: number;
  totalRows: number;
};

export interface GardnersChunkJobData {
  feed: GardnersFeed;
  logId: number;
  chunkKey: string;
  chunkIndex: number;
}

export type GardnersChunkJobResult = {
  processedRows: number;
  failedRows: number;
};
