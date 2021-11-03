import { z } from "zod";

export const ManifestV010 = z.object({
  manifest: z.string().nonempty(),
  version: z.string().nonempty(),
  index: z.object({ path: z.string(), ext: z.string().optional() }).nullish(),
  paths: z.record(z.object({ id: z.string(), ext: z.string().optional() })),
});

export type ManifestV010 = z.infer<typeof ManifestV010>;
