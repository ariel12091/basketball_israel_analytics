import { openApiSpec } from "@/server/openapi";
import { handleError } from "@/server/api";
import { logged } from "@/server/requestLog";
export const runtime = "nodejs";

/** GET /api/v1/openapi.json: the OpenAPI 3.1 description of /api/v1,
 * generated from the code that serves it (server/openapi.ts). */
async function get() {
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

export const GET = logged("openapi.json", get);
