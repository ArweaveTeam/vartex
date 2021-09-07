import { IGatsbyWorkerMessenger } from "../gatsby-worker/child";
import winston from "winston";
const { createLogger, transports, format } = winston;

export const log = createLogger({
  level: "info",
  transports: new transports.Console({
    format: format.simple(),
  }),
});

export const mkWorkerLog = (
  messenger: IGatsbyWorkerMessenger<unknown>
): ((message: string, context?: unknown) => void) => {
  return function (message: string, context?: unknown) {
    messenger.sendMessage({
      type: "log:info",
      message: `${message || ""}\n${
        typeof context === "object" ? JSON.stringify(context) : context || ""
      }`,
    });
  };
};
