export async function handler(event) {
    // Read from environment variables
    const METABASE_HOST   = process.env.METABASE_HOST;           
    const API_KEY         = process.env.METABASE_API_KEY;
    const DATABASE_ID     = Number(process.env.METABASE_DATABASE_ID);
    const PUBLISHER_NAME  = process.env.
    ;
  
    // Build the payload
    const payload = {
      database: DATABASE_ID,
      type: 'native',
      native: {
        query: `
          SELECT
            "public"."daily_data"."host_bandwidth"   AS "host_bandwidth",
            "public"."daily_data"."image_bandwidth"  AS "image_bandwidth",
            "public"."daily_data"."date"             AS "date"
          FROM "public"."daily_data"
          WHERE
            ("public"."daily_data"."date" = CAST(CAST((NOW() + INTERVAL '-1 day') AS date) AS timestamptz))
            AND ("public"."daily_data"."publisher_name" = '${PUBLISHER}')
          LIMIT 1
        `.replace(/\s+/g, ' ').trim()
      },
      parameters: []
    };
  
    // Call Metabase
    const resp = await fetch(`${METABASE_HOST}/api/dataset`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY
      },
      body: JSON.stringify(payload)
    });
  
    if (!resp.ok) {
      const text = await resp.text();
      return {
        statusCode: resp.status,
        body: JSON.stringify({ error: text })
      };
    }
  
    const data = await resp.json();
  
    // Extract the row
    const row = data?.data?.rows?.[0] || [];
    const result = {
      host_bandwidth: row[0] ?? null,
      image_bandwidth: row[1] ?? null,
      date: row[2] ?? null
    };
  
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(result)
    };
  }
  