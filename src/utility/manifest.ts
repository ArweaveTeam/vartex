import { z } from "zod";

export const ManifestV010 = z.object({
  manifest: z.string().nonempty(),
  version: z.string().nonempty(),
  index: z.object({ path: z.string() }).nullish(),
  paths: z.record(z.object({ id: z.string(), ext: z.string().optional() })),
});