import { IGatsbyWorkerMessenger } from "../gatsby-worker/child";
import {
  MessagesFromParent,
  MessagesFromWorker,
} from "../workers/message-types";
import winston from "winston";
const { createLogger, transports, format } = winston;

export const log = createLogger({
  level: "info",
  transports: new transports.Console({
    format: format.simple(),
  }),
});

export const mkWorkerLog = (
  messenger: IGatsbyWorkerMessenger<MessagesFromParent, MessagesFromWorker>
): any => {
  return function (message: string, context?: unknown) {
    context ? console.log(message, context) : console.log(message);
    if (messenger) {
      messenger.sendMessage({
        type: "log:info",
        message: `${message || ""}\n${
          typeof context === "object" ? JSON.stringify(context) : context || ""
        }`,
      });
    } else {
      context ? console.log(message, context) : console.log(message);
    }
  };
};
