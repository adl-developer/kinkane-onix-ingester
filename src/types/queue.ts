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
