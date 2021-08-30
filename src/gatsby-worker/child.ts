import {
  ParentMessageUnion,
  ChildMessageUnion,
  EXECUTE,
  END,
  ERROR,
  RESULT,
  CUSTOM_MESSAGE,
} from "./types";
import { isPromise } from "./utils";

export interface IGatsbyWorkerMessenger<
  MessagesFromParent = unknown,
  MessagesFromChild = MessagesFromParent
> {
  onMessage: (listener: (message: MessagesFromParent) => void) => void;
  sendMessage: (message: MessagesFromChild) => void;
  messagingVersion: 1;
}

/**
 * Used to check wether current context is executed in worker process
 */
let isWorker = false;
let getMessenger = function <
  MessagesFromParent = unknown,
  MessagesFromChild = MessagesFromParent
>(): IGatsbyWorkerMessenger<MessagesFromParent, MessagesFromChild> | undefined {
  return undefined;
};

if (process.send && process.env.GATSBY_WORKER_MODULE_PATH) {
  isWorker = true;
  const listeners: Array<(message: any) => void> = [];
  const ensuredSendToMain = process.send.bind(process) as (
    message: ChildMessageUnion
  ) => void;

  function onError(error: Error): void {
    if (error == undefined) {
      error = new Error(`"null" or "undefined" thrown`);
    }

    const message: ChildMessageUnion = [
      ERROR,
      error.constructor && error.constructor.name,
      error.message,
      error.stack,
      error,
    ];

    ensuredSendToMain(message);
  }

  function onResult(result: unknown): void {
    const message: ChildMessageUnion = [RESULT, result];
    ensuredSendToMain(message);
  }

  const MESSAGING_VERSION = 1;

  getMessenger = function <
    MessagesFromParent = unknown,
    MessagesFromChild = MessagesFromParent
  >(): IGatsbyWorkerMessenger<MessagesFromParent, MessagesFromChild> {
    return {
      onMessage(listener: (message: MessagesFromParent) => void): void {
        listeners.push(listener);
      },
      sendMessage(message: MessagesFromChild): void {
        const poolMessage: ChildMessageUnion = [CUSTOM_MESSAGE, message];
        ensuredSendToMain(poolMessage);
      },
      messagingVersion: MESSAGING_VERSION,
    };
  };

  import(process.env.GATSBY_WORKER_MODULE_PATH).then((child) => {
    function messageHandler(message: ParentMessageUnion): void {
      switch (message[0]) {
      case EXECUTE: {
        let result;
        try {
          result = child[message[1]].call(child, ...message[2]);
        } catch (error) {
          onError(error);
          return;
        }

        if (isPromise(result)) {
          result.then(onResult, onError);
        } else {
          onResult(result);
        }
      
      break;
      }
      case END: {
        process.off(`message`, messageHandler);
      
      break;
      }
      case CUSTOM_MESSAGE: {
        for (const listener of listeners) {
          listener(message[1]);
        }
      
      break;
      }
      // No default
      }
    }

    process.on(`message`, messageHandler);
  });
}

export { isWorker, getMessenger };
