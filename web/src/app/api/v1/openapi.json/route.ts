import { openApiSpec } from "@/server/openapi";
import { handleError } from "@/server/api";
export const runtime = "nodejs";

/** GET /api/v1/openapi.json: the OpenAPI 3.1 description of /api/v1,
 * generated from the code that serves it (server/openapi.ts). */
export async function GET() {
  try {
    return Response.json(openApiSpec(), {
      headers: {
        "Cache-Control": "public, max-age=300",
        // Changes only with a deploy, which clears the CDN.
        "Vercel-CDN-Cache-Control": "public, s-maxage=3600",
        "Access-Control-Allow-Origin": "*",
      },
    });
  } catch (error) {
    return handleError(error);
  }
}
