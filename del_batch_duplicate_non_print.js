import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

const checkTableExists = async (tableName) => {
  const result = await prisma.$queryRaw`SELECT EXISTS (
           SELECT 1 
           FROM information_schema.tables 
           WHERE table_schema = 'public' 
           AND table_name = ${tableName}
         ) AS exists;`;

  return result[0]?.exists || false;
};

// Generate all unique pairs
const getPairs = (arr) => {
  const pairs = [];
  for (let i = 0; i < arr.length; i++) {
    for (let j = i + 1; j < arr.length; j++) {
      pairs.push([arr[i].batch_id, arr[j].batch_id]);
    }
  }
  return pairs;
};

(async () => {
  try {
    const products = await prisma.productGenerationId.findMany({
      select: { generation_id: true },
    });
    const LEVELS = [0, 1, 2, 3];
    for (const level of LEVELS) {
      console.log("CURRENT LEVEL ", level);
      for (const element of products) {
        const tableName = `${element.generation_id.toLocaleLowerCase()}${level}_codes`;
        const exists = await checkTableExists(tableName);
        if (exists) {
          const duplicateInBatches = await prisma.$queryRawUnsafe(`
                            SELECT DISTINCT batch_id
                            FROM ${tableName}
                            WHERE unique_code IN (
                                SELECT unique_code
                                FROM ${tableName}
                                GROUP BY unique_code
                                HAVING COUNT(DISTINCT batch_id) > 1
                            )
                        `);
          if (duplicateInBatches.length > 0) {
            console.log("duplicateInBatches ", duplicateInBatches);

            // Remove duplicate codes in same batch printed = false AND is_scanned = false AND is_aggregated = false AND is_dropped = false
            for (const batch of duplicateInBatches) {
              await prisma.$executeRawUnsafe(`
                DELETE FROM ${tableName}
                WHERE unique_code IN (
                    SELECT unique_code FROM ${tableName} WHERE unique_code IN (
                        SELECT unique_code
                        FROM ${tableName}
                        WHERE batch_id = '${batch.batch_id}'
                        GROUP BY unique_code
                        HAVING COUNT(*) > 1
                    )
                )
                AND printed = false AND is_scanned = false AND is_aggregated = false AND is_dropped = false
               `);
            }

            // Remove accross batches
            const pairs = getPairs(duplicateInBatches);
            // Generate SQL queries
            const queries = pairs.flatMap(([id1, id2]) => {
              const sharedSubquery = `
                WITH target_codes AS (
                    SELECT unique_code
                    FROM ${tableName}
                    WHERE batch_id IN ('${id1}', '${id2}')
                    GROUP BY unique_code
                    HAVING COUNT(DISTINCT batch_id) > 1
                )`;

              const baseQuery = (batchId) =>
                `${sharedSubquery} 
                    DELETE FROM ${tableName} WHERE 
                    batch_id = '${batchId}' AND 
                    unique_code IN (SELECT unique_code FROM target_codes) 
                    AND printed = false 
                    AND is_scanned = false 
                    AND is_aggregated = false 
                    AND is_dropped = false
                `.trim();

              return [baseQuery(id1), baseQuery(id2)];
            });
            queries.forEach(async (query, index) => {
                await prisma.$executeRawUnsafe(query);
            });
          }
        }
      }
    }
  } catch (err) {
    console.error("❌ Error correction of batch tables:", err);
  } finally {
    await prisma.$disconnect();
  }
})();
