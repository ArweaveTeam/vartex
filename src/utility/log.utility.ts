import winston from 'winston';

const { createLogger, transports, format } = winston;

export const log = createLogger({
  level: 'info',
  transports: new transports.Console({
    format: format.simple(),
  }),
});
