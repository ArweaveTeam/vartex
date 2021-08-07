import { config } from "dotenv";
import { read, exists } from "fs-jetpack";
import { Request, Response } from "express";
// import { connection } from '../database/connection.database';
import { ManifestV1 } from "../types/manifest.types";
import { log } from "../utility/log.utility";
import { getTransaction, tagValue } from "../query/transaction.query";
// import { cacheFolder, cacheFile, cacheAnsFile } from '../caching/file.caching';

process.env.NODE_ENV !== "test" && config();
