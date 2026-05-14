import sax from 'sax';
import { Readable } from 'stream';
import { OnixProduct, OnixContributor, OnixSubject, OnixPrice } from '../types/onix';

function parseOnixDate(raw: string | null): string | null {
  if (!raw || raw.length < 8) return null;
  return `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
}

function blankProduct(): OnixProduct {
  return {
    recordReference: '',
    notificationType: '',
    isbn13: null,
    productForm: null,
    productComposition: null,
    editionNumber: null,
    pageCount: null,
    heightMm: null,
    widthMm: null,
    thicknessMm: null,
    weightGr: null,
    countryOfManufacture: null,
    productClassificationCode: null,
    title: '',
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
  };
}

/**
 * Streams an ONIX 3.x XML file and yields parsed products in batches.
 *
 * Backpressure: when more than `maxPending` batches are queued the source
 * stream is paused so the SAX parser stops reading from R2.  The stream
 * resumes as soon as the generator consumer pulls a batch back down below
 * the threshold.  Memory ceiling ≈ maxPending × batchSize × ~5 KB.
 */
export async function* parseOnixStream(
  stream: Readable,
  batchSize: number,
  maxPending = 5,
): AsyncGenerator<OnixProduct[]> {
  const pending: OnixProduct[][] = [];
  let streamPaused = false;
  let done = false;
  let parseError: Error | null = null;
  let wake: (() => void) | null = null;

  function signal() {
    if (wake) {
      const fn = wake;
      wake = null;
      fn();
    }
  }

  function maybePause() {
    if (!streamPaused && pending.length >= maxPending) {
      stream.pause();
      streamPaused = true;
    }
  }

  function maybeResume() {
    if (streamPaused && pending.length < maxPending) {
      stream.resume();
      streamPaused = false;
    }
  }

  // ── SAX setup ─────────────────────────────────────────────────────────────
  const saxStream = sax.createStream(true, { trim: false, normalize: false });

  let current: OnixProduct | null = null;
  const batch: OnixProduct[] = [];

  let currentId: { type: string; value: string } | null = null;
  let currentContrib: Partial<OnixContributor> | null = null;
  let currentSubject: Partial<OnixSubject> | null = null;
  let currentExtent: { type: string; value: number } | null = null;
  let currentMeasure: { type: string; value: number } | null = null;
  let currentText: { type: string; text: string } | null = null;
  let currentDate: { role: string; date: string } | null = null;
  let currentPrice: Partial<OnixPrice> | null = null;
  let currentTax: { rateCode: string; ratePercent: number } | null = null;
  let charBuf = '';

  saxStream.on('opentag', (node) => {
    charBuf = '';
    const n = node.name;
    if (n === 'Product') current = blankProduct();
    else if (n === 'ProductIdentifier') currentId = { type: '', value: '' };
    else if (n === 'Contributor') currentContrib = {};
    else if (n === 'Subject') currentSubject = { isMainSubject: false };
    else if (n === 'MainSubject') { if (currentSubject) currentSubject.isMainSubject = true; }
    else if (n === 'Extent') currentExtent = { type: '', value: 0 };
    else if (n === 'Measure') currentMeasure = { type: '', value: 0 };
    else if (n === 'TextContent') currentText = { type: '', text: '' };
    else if (n === 'PublishingDate') currentDate = { role: '', date: '' };
    else if (n === 'Price') currentPrice = {};
    else if (n === 'Tax') currentTax = { rateCode: '', ratePercent: 0 };
  });

  saxStream.on('text', (text) => { charBuf += text; });
  saxStream.on('cdata', (cdata) => { charBuf += cdata; });

  saxStream.on('closetag', (n) => {
    const text = charBuf.trim();
    charBuf = '';

    if (!current && n !== 'ONIXMessage' && n !== 'Header') return;

    switch (n) {
      case 'Product': {
        if (current && current.title) {
          batch.push(current);
          if (batch.length >= batchSize) {
            pending.push([...batch]);
            batch.length = 0;
            maybePause();
            signal();
          }
        }
        current = null;
        break;
      }
      case 'RecordReference': if (current) current.recordReference = text; break;
      case 'NotificationType': if (current) current.notificationType = text; break;

      case 'ProductIDType': if (currentId) currentId.type = text; break;
      case 'IDValue': if (currentId) currentId.value = text; break;
      case 'ProductIdentifier':
        if (currentId && currentId.type === '15' && current) current.isbn13 = currentId.value || null;
        currentId = null; break;

      case 'ProductComposition': if (current) current.productComposition = text; break;
      case 'ProductForm': if (current) current.productForm = text; break;
      case 'CountryOfManufacture': if (current) current.countryOfManufacture = text; break;
      case 'ProductClassificationCode': if (current) current.productClassificationCode = text; break;
      case 'TitleText': if (current) current.title = text; break;
      case 'Subtitle': if (current) current.subtitle = text || null; break;
      case 'EditionNumber': if (current) current.editionNumber = parseInt(text, 10) || null; break;

      case 'ExtentType': if (currentExtent) currentExtent.type = text; break;
      case 'ExtentValue': if (currentExtent) currentExtent.value = parseFloat(text); break;
      case 'Extent':
        if (currentExtent && currentExtent.type === '00' && current) current.pageCount = currentExtent.value || null;
        currentExtent = null; break;

      case 'MeasureType': if (currentMeasure) currentMeasure.type = text; break;
      case 'Measurement': if (currentMeasure) currentMeasure.value = parseFloat(text); break;
      case 'Measure':
        if (currentMeasure && current) {
          const v = currentMeasure.value;
          if (currentMeasure.type === '01') current.heightMm = v;
          else if (currentMeasure.type === '02') current.widthMm = v;
          else if (currentMeasure.type === '03') current.thicknessMm = v;
          else if (currentMeasure.type === '08') current.weightGr = v;
        }
        currentMeasure = null; break;

      case 'SequenceNumber': if (currentContrib) currentContrib.sequenceNumber = parseInt(text, 10) || null; break;
      case 'ContributorRole': if (currentContrib) currentContrib.role = text; break;
      case 'PersonName': if (currentContrib) currentContrib.personName = text; break;
      case 'PersonNameInverted': if (currentContrib) currentContrib.personNameInverted = text; break;
      case 'Contributor':
        if (currentContrib && current) current.contributors.push(currentContrib as OnixContributor);
        currentContrib = null; break;

      case 'SubjectSchemeIdentifier': if (currentSubject) currentSubject.schemeIdentifier = text; break;
      case 'SubjectSchemeVersion': if (currentSubject) currentSubject.schemeVersion = text; break;
      case 'SubjectCode': if (currentSubject) currentSubject.subjectCode = text; break;
      case 'SubjectHeadingText': if (currentSubject) currentSubject.subjectHeadingText = text; break;
      case 'Subject':
        if (currentSubject && current) current.subjects.push(currentSubject as OnixSubject);
        currentSubject = null; break;

      case 'TextType': if (currentText) currentText.type = text; break;
      case 'Text': if (currentText) currentText.text = text; break;
      case 'TextContent':
        if (currentText && current) {
          if (currentText.type === '02') current.shortDescription = currentText.text || null;
          else if (currentText.type === '03') current.longDescription = currentText.text || null;
        }
        currentText = null; break;

      case 'ImprintName': if (current) current.imprintName = text; break;
      case 'PublisherName': if (current) current.publisherName = text; break;
      case 'CountryOfPublication': if (current) current.countryOfPublication = text; break;
      case 'PublishingStatus': if (current) current.publishingStatus = text; break;
      case 'PublishingDateRole': if (currentDate) currentDate.role = text; break;
      case 'Date': if (currentDate) currentDate.date = text; break;
      case 'PublishingDate':
        if (currentDate && currentDate.role === '01' && current) current.publicationDate = parseOnixDate(currentDate.date);
        currentDate = null; break;

      case 'ProductAvailability': if (current) current.availabilityCode = text; break;
      case 'ReturnsCode': if (current) current.returnsCode = text; break;
      case 'OrderTime': if (current) current.orderTime = parseInt(text, 10) || null; break;

      case 'PriceType': if (currentPrice) currentPrice.priceType = text; break;
      case 'PriceAmount': if (currentPrice) currentPrice.priceAmount = parseFloat(text) || null; break;
      case 'CurrencyCode': if (currentPrice) currentPrice.currencyCode = text; break;
      case 'TaxRateCode': if (currentTax) currentTax.rateCode = text; break;
      case 'TaxRatePercent': if (currentTax) currentTax.ratePercent = parseFloat(text); break;
      case 'Tax':
        if (currentTax && currentPrice) {
          currentPrice.taxRateCode = currentTax.rateCode || null;
          currentPrice.taxRatePercent = currentTax.ratePercent ?? null;
        }
        currentTax = null; break;
      case 'Price':
        if (currentPrice && current) current.prices.push(currentPrice as OnixPrice);
        currentPrice = null; break;
    }
  });

  saxStream.on('end', () => {
    if (batch.length > 0) {
      pending.push([...batch]);
      batch.length = 0;
    }
    done = true;
    signal();
  });

  saxStream.on('error', (err) => {
    parseError = err instanceof Error ? err : new Error(String(err));
    done = true;
    signal();
  });

  stream.pipe(saxStream);

  // ── Async generator consumer ───────────────────────────────────────────────
  while (!done || pending.length > 0) {
    if (pending.length > 0) {
      yield pending.shift()!;
      maybeResume(); // unpause stream now that we consumed a batch
    } else {
      await new Promise<void>((res) => { wake = res; });
    }
  }

  if (parseError) throw parseError;
}
