export interface OnixProduct {
  recordReference: string;
  notificationType: string;
  isbn13: string | null;
  productForm: string | null;
  productComposition: string | null;
  editionNumber: number | null;
  pageCount: number | null;
  heightMm: number | null;
  widthMm: number | null;
  thicknessMm: number | null;
  weightGr: number | null;
  countryOfManufacture: string | null;
  productClassificationCode: string | null;
  title: string;
  subtitle: string | null;
  shortDescription: string | null;
  longDescription: string | null;
  publisherName: string | null;
  imprintName: string | null;
  countryOfPublication: string | null;
  publishingStatus: string | null;
  publicationDate: string | null; // YYYY-MM-DD
  availabilityCode: string | null;
  returnsCode: string | null;
  orderTime: number | null;
  contributors: OnixContributor[];
  subjects: OnixSubject[];
  prices: OnixPrice[];
}

export interface OnixContributor {
  sequenceNumber: number | null;
  role: string | null;
  personName: string | null;
  personNameInverted: string | null;
}

export interface OnixSubject {
  schemeIdentifier: string | null;
  schemeVersion: string | null;
  subjectCode: string | null;
  subjectHeadingText: string | null;
  isMainSubject: boolean;
}

export interface OnixPrice {
  priceType: string | null;
  priceAmount: number | null;
  currencyCode: string | null;
  taxRateCode: string | null;
  taxRatePercent: number | null;
}
