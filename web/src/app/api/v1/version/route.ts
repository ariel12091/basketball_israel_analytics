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
          "Cache-Control": "public, max-age=0",
          "Vercel-CDN-Cache-Control": "s-maxage=60",
        },
      },
    );
  } catch (error) {
    return handleError(error);
  }
}
