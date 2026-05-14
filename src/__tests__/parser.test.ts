import { describe, it, expect } from 'vitest';
import { Readable } from 'stream';
import { parseOnixStream } from '../services/parser.service';

function makeStream(xml: string): Readable {
  return Readable.from([xml]);
}

const MINIMAL_ONIX = `<?xml version="1.0" encoding="UTF-8"?>
<ONIXMessage release="3.1">
  <Header><Sender><SenderName>Test</SenderName></Sender></Header>
  <Product>
    <RecordReference>REF001</RecordReference>
    <NotificationType>03</NotificationType>
    <ProductIdentifier>
      <ProductIDType>15</ProductIDType>
      <IDValue>9781234567890</IDValue>
    </ProductIdentifier>
    <DescriptiveDetail>
      <ProductComposition>00</ProductComposition>
      <ProductForm>BB</ProductForm>
      <TitleDetail>
        <TitleType>01</TitleType>
        <TitleElement>
          <TitleElementLevel>01</TitleElementLevel>
          <TitleText>The Great Test Book</TitleText>
          <Subtitle>A Subtitle</Subtitle>
        </TitleElement>
      </TitleDetail>
      <Contributor>
        <SequenceNumber>1</SequenceNumber>
        <ContributorRole>A01</ContributorRole>
        <PersonName>Jane Author</PersonName>
        <PersonNameInverted>Author, Jane</PersonNameInverted>
      </Contributor>
      <Extent>
        <ExtentType>00</ExtentType>
        <ExtentValue>320</ExtentValue>
        <ExtentUnit>03</ExtentUnit>
      </Extent>
      <Subject>
        <SubjectSchemeIdentifier>93</SubjectSchemeIdentifier>
        <SubjectCode>FIC000000</SubjectCode>
        <SubjectHeadingText>Fiction</SubjectHeadingText>
      </Subject>
    </DescriptiveDetail>
    <CollateralDetail>
      <TextContent>
        <TextType>02</TextType>
        <ContentAudience>00</ContentAudience>
        <Text>A short description.</Text>
      </TextContent>
      <TextContent>
        <TextType>03</TextType>
        <ContentAudience>00</ContentAudience>
        <Text>A longer description of the book.</Text>
      </TextContent>
    </CollateralDetail>
    <PublishingDetail>
      <Publisher>
        <PublisherName>Test Publisher</PublisherName>
      </Publisher>
      <PublishingStatus>04</PublishingStatus>
      <PublishingDate>
        <PublishingDateRole>01</PublishingDateRole>
        <Date>20240315</Date>
      </PublishingDate>
    </PublishingDetail>
    <ProductSupply>
      <SupplyDetail>
        <ProductAvailability>20</ProductAvailability>
        <Price>
          <PriceType>02</PriceType>
          <PriceAmount>19.99</PriceAmount>
          <CurrencyCode>GBP</CurrencyCode>
        </Price>
      </SupplyDetail>
    </ProductSupply>
  </Product>
</ONIXMessage>`;

const DELETE_NOTIFICATION = `<?xml version="1.0" encoding="UTF-8"?>
<ONIXMessage release="3.1">
  <Header><Sender><SenderName>Test</SenderName></Sender></Header>
  <Product>
    <RecordReference>REF002</RecordReference>
    <NotificationType>05</NotificationType>
    <ProductIdentifier>
      <ProductIDType>15</ProductIDType>
      <IDValue>9780000000001</IDValue>
    </ProductIdentifier>
    <DescriptiveDetail>
      <ProductComposition>00</ProductComposition>
      <ProductForm>BB</ProductForm>
      <TitleDetail>
        <TitleType>01</TitleType>
        <TitleElement>
          <TitleElementLevel>01</TitleElementLevel>
          <TitleText>Book To Delete</TitleText>
        </TitleElement>
      </TitleDetail>
    </DescriptiveDetail>
  </Product>
</ONIXMessage>`;

async function collectBatches(xml: string, batchSize = 500): Promise<import('../types/onix').OnixProduct[][]> {
  const batches: import('../types/onix').OnixProduct[][] = [];
  for await (const batch of parseOnixStream(makeStream(xml), batchSize)) {
    batches.push(batch);
  }
  return batches;
}

describe('parseOnixStream', () => {
  it('parses a single product into one batch', async () => {
    const batches = await collectBatches(MINIMAL_ONIX);
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(1);
  });

  it('maps core book fields correctly', async () => {
    const [[book]] = await collectBatches(MINIMAL_ONIX);
    expect(book.recordReference).toBe('REF001');
    expect(book.notificationType).toBe('03');
    expect(book.isbn13).toBe('9781234567890');
    expect(book.title).toBe('The Great Test Book');
    expect(book.subtitle).toBe('A Subtitle');
    expect(book.productForm).toBe('BB');
    expect(book.pageCount).toBe(320);
    expect(book.publisherName).toBe('Test Publisher');
    expect(book.publishingStatus).toBe('04');
    expect(book.publicationDate).toBe('2024-03-15');
    expect(book.availabilityCode).toBe('20');
  });

  it('maps descriptions correctly', async () => {
    const [[book]] = await collectBatches(MINIMAL_ONIX);
    expect(book.shortDescription).toBe('A short description.');
    expect(book.longDescription).toBe('A longer description of the book.');
  });

  it('maps contributors correctly', async () => {
    const [[book]] = await collectBatches(MINIMAL_ONIX);
    expect(book.contributors).toHaveLength(1);
    expect(book.contributors[0]).toMatchObject({
      sequenceNumber: 1,
      role: 'A01',
      personName: 'Jane Author',
      personNameInverted: 'Author, Jane',
    });
  });

  it('maps subjects correctly', async () => {
    const [[book]] = await collectBatches(MINIMAL_ONIX);
    expect(book.subjects).toHaveLength(1);
    expect(book.subjects[0]).toMatchObject({
      schemeIdentifier: '93',
      subjectCode: 'FIC000000',
      subjectHeadingText: 'Fiction',
    });
  });

  it('maps prices correctly', async () => {
    const [[book]] = await collectBatches(MINIMAL_ONIX);
    expect(book.prices).toHaveLength(1);
    expect(book.prices[0]).toMatchObject({
      priceType: '02',
      priceAmount: 19.99,
      currencyCode: 'GBP',
    });
  });

  it('parses delete notification (type 05)', async () => {
    const [[book]] = await collectBatches(DELETE_NOTIFICATION);
    expect(book.notificationType).toBe('05');
    expect(book.recordReference).toBe('REF002');
  });

  it('batches correctly when batchSize is 1', async () => {
    const xml = `<?xml version="1.0"?>
    <ONIXMessage release="3.1">
      <Header><Sender><SenderName>T</SenderName></Sender></Header>
      <Product>
        <RecordReference>A</RecordReference><NotificationType>03</NotificationType>
        <DescriptiveDetail><ProductComposition>00</ProductComposition><ProductForm>BB</ProductForm>
          <TitleDetail><TitleType>01</TitleType><TitleElement><TitleElementLevel>01</TitleElementLevel>
            <TitleText>Book A</TitleText></TitleElement></TitleDetail>
        </DescriptiveDetail>
      </Product>
      <Product>
        <RecordReference>B</RecordReference><NotificationType>03</NotificationType>
        <DescriptiveDetail><ProductComposition>00</ProductComposition><ProductForm>BB</ProductForm>
          <TitleDetail><TitleType>01</TitleType><TitleElement><TitleElementLevel>01</TitleElementLevel>
            <TitleText>Book B</TitleText></TitleElement></TitleDetail>
        </DescriptiveDetail>
      </Product>
    </ONIXMessage>`;

    const batches = await collectBatches(xml, 1);
    expect(batches).toHaveLength(2);
    expect(batches[0][0].title).toBe('Book A');
    expect(batches[1][0].title).toBe('Book B');
  });

  it('skips products with no title', async () => {
    const xml = `<?xml version="1.0"?>
    <ONIXMessage release="3.1">
      <Header><Sender><SenderName>T</SenderName></Sender></Header>
      <Product>
        <RecordReference>NOTITLE</RecordReference><NotificationType>03</NotificationType>
        <DescriptiveDetail><ProductComposition>00</ProductComposition><ProductForm>BB</ProductForm>
          <TitleDetail><TitleType>01</TitleType><TitleElement><TitleElementLevel>01</TitleElementLevel>
            <TitleText></TitleText></TitleElement></TitleDetail>
        </DescriptiveDetail>
      </Product>
    </ONIXMessage>`;

    const batches = await collectBatches(xml);
    expect(batches).toHaveLength(0);
  });

  it('throws on malformed XML', async () => {
    await expect(collectBatches('<ONIXMessage><unclosed>')).rejects.toThrow();
  });
});
