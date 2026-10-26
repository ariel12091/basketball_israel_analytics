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
          // Shared by every visitor: the CDN answers polls for 10 s at a time,
          // so a publication is seen at most 10 s later than the database has it.
          "Vercel-CDN-Cache-Control": "public, s-maxage=10",
        },
      },
    );
  } catch (error) {
    return handleError(error);
  }
}
