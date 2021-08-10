import KoiLogs from "koi-logs";
import { Request, Response } from "express";

export const koiLogger = new KoiLogs("./");

export async function koiLogsRoute(request: Request, res: Response) {
  return koiLogger.koiLogsHelper(request, res);
}

export async function koiLogsRawRoute(request: Request, res: Response) {
  return koiLogger.koiRawLogsHelper(request, res);
}
