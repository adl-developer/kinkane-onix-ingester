import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';
import { fileQueue, chunkQueue } from './index';

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/bull-board');

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const asAdapter = (a: BullMQAdapter): any => a;

createBullBoard({
  queues: [asAdapter(new BullMQAdapter(fileQueue)), asAdapter(new BullMQAdapter(chunkQueue))],
  serverAdapter,
});

export const bullBoardRouter = serverAdapter.getRouter();
