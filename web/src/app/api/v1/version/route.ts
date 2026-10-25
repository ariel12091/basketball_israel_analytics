import { withSnapshot, Deadline } from "@/server/db";
import { readVersion } from "@/server/gameLogsRepo";
import { handleError } from "@/server/api";
export const runtime = "nodejs";
export async function GET() {
  try {
    return Response.json(
      await withSnapshot(new Deadline(), async (q) => {
        const il = await readVersion(q, "il"),
          euro = await readVersion(q, "euroleague");
        return {
          leagues: [
            { league: "il", version: il },
            { league: "euroleague", version: euro },
            { league: "eurocup", version: euro },
          ],
        };
      }),
      {
        headers: {
          "Cache-Control": "no-store",
          "Vercel-CDN-Cache-Control": "no-store",
        },
      },
    );
  } catch (error) {
    return handleError(error);
  }
}
