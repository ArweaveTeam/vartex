import pWaitFor from "p-wait-for";
import pMinDelay from "p-min-delay";
import pWhilst from "p-whilst";
import exitHook from "exit-hook";
import { importManifests } from "../src/workers/import-manifest";

let exitSignaled = false;

exitHook(() => {
  exitSignaled = true;
});

(async () => {
  console.log("starting import-manifests worker...");

  pWhilst(
    () => !exitSignaled,
    async () => {
      try {
        await (pMinDelay as any)(importManifests(), 120 * 1000);
      } catch (error) {
        console.error(error);
      }
    }
  );
})();
