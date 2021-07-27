import bodyParser from 'body-parser';

export const jsonMiddleware = bodyParser.json({
  limit: '15mb',
  type: () => true,
});
