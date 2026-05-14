import { describe, it, expect } from 'vitest';
import { embeddingService } from '../services/embedding.service';
import { OnixProduct } from '../types/onix';

function makeBook(overrides: Partial<OnixProduct> = {}): OnixProduct {
  return {
    recordReference: 'REF001',
    notificationType: '03',
    isbn13: '9781234567890',
    productForm: 'BB',
    productComposition: '00',
    editionNumber: null,
    pageCount: null,
    heightMm: null,
    widthMm: null,
    thicknessMm: null,
    weightGr: null,
    countryOfManufacture: null,
    productClassificationCode: null,
    title: 'Test Book',
    subtitle: null,
    shortDescription: null,
    longDescription: null,
    publisherName: null,
    imprintName: null,
    countryOfPublication: null,
    publishingStatus: null,
    publicationDate: null,
    availabilityCode: null,
    returnsCode: null,
    orderTime: null,
    contributors: [],
    subjects: [],
    prices: [],
    ...overrides,
  };
}

describe('embeddingService.buildBookText', () => {
  it('includes title', () => {
    const text = embeddingService.buildBookText(makeBook({ title: 'My Book' }));
    expect(text).toContain('My Book');
  });

  it('includes subtitle when present', () => {
    const text = embeddingService.buildBookText(makeBook({ subtitle: 'The Subtitle' }));
    expect(text).toContain('The Subtitle');
  });

  it('includes author name with "By" prefix', () => {
    const text = embeddingService.buildBookText(
      makeBook({
        contributors: [
          { sequenceNumber: 1, role: 'A01', personName: 'Jane Author', personNameInverted: 'Author, Jane' },
        ],
      }),
    );
    expect(text).toContain('By Jane Author');
  });

  it('excludes non-author contributors from author line', () => {
    const text = embeddingService.buildBookText(
      makeBook({
        contributors: [
          { sequenceNumber: 1, role: 'B01', personName: 'Ed Itor', personNameInverted: 'Itor, Ed' },
        ],
      }),
    );
    expect(text).not.toContain('By Ed Itor');
  });

  it('includes subject heading text', () => {
    const text = embeddingService.buildBookText(
      makeBook({
        subjects: [
          { schemeIdentifier: '93', schemeVersion: null, subjectCode: 'FIC', subjectHeadingText: 'Fiction', isMainSubject: true },
        ],
      }),
    );
    expect(text).toContain('Fiction');
  });

  it('uses shortDescription when longDescription is absent', () => {
    const text = embeddingService.buildBookText(
      makeBook({ shortDescription: 'A short one.', longDescription: null }),
    );
    expect(text).toContain('A short one.');
  });

  it('truncates longDescription to 500 chars when used as fallback', () => {
    const long = 'x'.repeat(1000);
    const text = embeddingService.buildBookText(
      makeBook({ shortDescription: null, longDescription: long }),
    );
    expect(text).toContain('x'.repeat(500));
    expect(text).not.toContain('x'.repeat(501));
  });

  it('produces a non-empty string for a minimal book', () => {
    const text = embeddingService.buildBookText(makeBook());
    expect(text.trim().length).toBeGreaterThan(0);
  });
});
