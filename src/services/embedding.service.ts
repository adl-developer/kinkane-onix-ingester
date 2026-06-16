import { GoogleGenerativeAI } from '@google/generative-ai';
import { config } from '../config';
import { OnixProduct } from '../types/onix';

class EmbeddingService {
  private readonly genai: GoogleGenerativeAI;

  constructor() {
    this.genai = new GoogleGenerativeAI(config.gemini.apiKey);
  }

  buildBookText(book: OnixProduct): string {
    const authors = book.contributors
      .filter((c) => c.role === 'A01')
      .map((c) => c.personName)
      .filter(Boolean)
      .join(', ');

    const subjects = book.subjects
      .map((s) => s.subjectHeadingText)
      .filter(Boolean)
      .join(', ');

    const parts = [
      book.title,
      book.subtitle,
      authors ? `By ${authors}` : null,
      subjects,
      book.shortDescription ?? book.longDescription?.slice(0, 500),
    ].filter(Boolean);

    return parts.join('. ');
  }

  /**
   * Generates embeddings for an array of text strings.
   * Batches requests to stay within API limits and handles retries.
   */
  async generateBatch(texts: string[]): Promise<number[][]> {
    const model = this.genai.getGenerativeModel({
      model: config.gemini.embeddingModel,
    });

    const results: number[][] = new Array(texts.length);
    const batchSize = config.ingestion.embeddingBatchSize;
    const delayMs = config.ingestion.embeddingBatchDelayMs;

    for (let i = 0; i < texts.length; i += batchSize) {
      if (i > 0 && delayMs > 0) {
        await new Promise((res) => setTimeout(res, delayMs));
      }

      const slice = texts.slice(i, i + batchSize);

      const response = await this.withRetry(() =>
        model.batchEmbedContents({
          requests: slice.map((text) => ({
            content: { parts: [{ text }], role: 'user' },
          })),
        }),
      );

      for (let j = 0; j < response.embeddings.length; j++) {
        results[i + j] = response.embeddings[j].values;
      }
    }

    return results;
  }

  private async withRetry<T>(fn: () => Promise<T>, maxAttempts = 5): Promise<T> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await fn();
      } catch (err) {
        lastError = err;
        if (attempt < maxAttempts) {
          // If Gemini returns a retry-after hint in the error message, honour it;
          // otherwise fall back to exponential back-off (1s, 2s, 4s, 8s …).
          const retryAfterMatch = String(err).match(/retryDelay["\s:]+(\d+)s/);
          const delay = retryAfterMatch
            ? (parseInt(retryAfterMatch[1], 10) + 2) * 1000
            : 1000 * 2 ** (attempt - 1);
          await new Promise((res) => setTimeout(res, delay));
        }
      }
    }
    throw lastError;
  }
}

export const embeddingService = new EmbeddingService();
