import { curry } from "rambda";
import winston from "winston";
const { createLogger, transports, format } = winston;

export const log = createLogger({
  level: "info",
  transports: new transports.Console({
    format: format.simple(),
  }),
});

export const mkWorkerLog = (messenger: any) => {
  return function (msg: string, ctx?: any) {
    messenger.sendMessage({
      type: "log:info",
      message: `${msg}\n${typeof ctx === "object" ? JSON.stringify(ctx) : ctx}`,
    });
  };
};
