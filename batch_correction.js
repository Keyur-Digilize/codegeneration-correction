import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

const checkTableExists = async (tableName, tx = prisma) => {
  const result = await tx.$queryRaw`SELECT EXISTS (
           SELECT 1 
           FROM information_schema.tables 
           WHERE table_schema = 'public' 
           AND table_name = ${tableName}
         ) AS exists;`;

  return result[0]?.exists || false;
};

const createDynamicTable = async (tableName, tx) => {
  try {
    const createTableQuery = `
            CREATE TABLE IF NOT EXISTS "${tableName}" (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                serial_no INT NOT NULL UNIQUE REFERENCES "CodesGenerated"(id) ON DELETE CASCADE,
                product_id UUID NOT NULL REFERENCES product(id) ON DELETE CASCADE,
                batch_id UUID NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
                unique_code VARCHAR(255) NOT NULL,
                location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
                code_gen_id VARCHAR(255) NOT NULL,
                country_code VARCHAR(1000) NOT NULL,
                printed BOOLEAN DEFAULT FALSE,
                is_scanned BOOLEAN DEFAULT FALSE,
                is_aggregated BOOLEAN DEFAULT FALSE,
                is_dropped BOOLEAN DEFAULT FALSE,
                parent_id UUID DEFAULT NULL,
                sent_to_cloud BOOLEAN DEFAULT FALSE,
                dropout_reason VARCHAR(20) DEFAULT NULL,
                is_scanned_in_order BOOLEAN DEFAULT FALSE,
                storage_bin INTEGER,
                in_transit BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            );
        `;
    await tx.$executeRawUnsafe(createTableQuery);
    console.log(`Table ${tableName} created successfully.`);
  } catch (error) {
    console.error("Error creating table:", error);
    throw error;
  }
};

(async () => {
  try {
    const LEVELS = [0, 1, 2, 3, 5];
    for (const level of LEVELS) {
     console.log("CURRENT LEVEL ", level);
     
      const codeReqs = await prisma.$queryRawUnsafe(`
            SELECT 
            batch_id,
            product_id,
            SUM(CAST(no_of_codes AS INTEGER)) AS total_requested_codes
            FROM public."CodeGenerationRequest"
            WHERE packaging_hierarchy = 'level${level}'
            GROUP BY batch_id, product_id
            HAVING COUNT(DISTINCT product_id) = 1 
            ORDER BY batch_id DESC;
            `);

      for (const req of codeReqs) {
        await prisma.$transaction(async (tx) => {
          const prodgen = await prisma.productGenerationId.findFirst({
            where: { product_id: req.product_id },
            select: { generation_id: true },
          });
          const product = await prisma.product.findFirst({ 
            where: { id: req.product },
            select: { productNumber_print: true, firstLayer_print: true, secondLayer_print: true, thirdLayer_print: true }
          });
          const tableName = `${prodgen.generation_id.toLocaleLowerCase()}${level}_codes`;
          const exists = await checkTableExists(tableName);
          const int_total_requested_codes = Number(req.total_requested_codes);
          if (exists) {
            const total_generated_codes = await prisma.$queryRawUnsafe(
              `SELECT COUNT(*) FROM "${tableName}" WHERE batch_id = '${req.batch_id}'`
            );
            const int_total_generated_codes = Number(
              total_generated_codes[0].count
            );
            // console.log("Total requested | generated ", Number(req.total_requested_codes), Number(total_generated_codes[0].count));
            if (int_total_generated_codes > int_total_requested_codes) {
              const printed = await prisma.$queryRawUnsafe(`
                            SELECT count(*) FROM "${tableName}" where batch_id = '${req.batch_id}' and printed = true
                        `);

              const keepNonPrinted =
                int_total_requested_codes - Number(printed[0].count);

              console.log(
                "Table Batch Requsted Generated Printed KeepNoPrinted ",
                tableName,
                req.batch_id,
                int_total_requested_codes,
                int_total_generated_codes,
                Number(printed[0].count),
                keepNonPrinted
              );

              await tx.$queryRawUnsafe(`
                  DELETE FROM ${tableName}
                  WHERE id IN (
                      SELECT id FROM ${tableName} where batch_id = '${req.batch_id}' and printed = false and is_scanned = false and is_aggregated = false and is_dropped = false
                      ORDER BY serial_no asc
                      OFFSET ${keepNonPrinted}
                  )
              `);
            }
          } else {
            if (level === 5) {
                const total_generated_codes = await prisma.$queryRawUnsafe(
                    `SELECT COUNT(*) FROM sscc_codes WHERE batch_id = '${req.batch_id}'`
                );
                const int_total_generated_codes = Number(
                    total_generated_codes[0].count
                );
                if (int_total_requested_codes !== int_total_generated_codes) {
                    console.log(`SSCC require not generated for batch ${req.batch_id}`);
                    
                }
                return;
            }
            if (product.productNumber_print && level === 0) {
                console.log(tableName, " does not exits ");
                await createDynamicTable(`${prodgen.generation_id.toLocaleLowerCase()}0_codes`, tx);
                await prisma.codeGenerationRequest.updateMany({ 
                    where: { 
                        batch_id: req.batch_id, 
                        product_id: 
                        req.product_id, 
                        packaging_hierarchy: `level${level}` 
                    },
                    data: { status: 'requested' }
                });
            } else if (product.firstLayer_print && level === 1){
                console.log(tableName, " does not exits ");
                await createDynamicTable(`${prodgen.generation_id.toLocaleLowerCase()}1_codes`, tx);
                 await prisma.codeGenerationRequest.updateMany({ 
                    where: { 
                        batch_id: req.batch_id, 
                        product_id: 
                        req.product_id, 
                        packaging_hierarchy: `level${level}` 
                    },
                    data: { status: 'requested' }
                });
            } else if (product.secondLayer_print && level === 2) {
                console.log(tableName, " does not exits ");
                await createDynamicTable(`${prodgen.generation_id.toLocaleLowerCase()}2_codes`, tx);
                await prisma.codeGenerationRequest.updateMany({ 
                    where: { 
                        batch_id: req.batch_id, 
                        product_id: 
                        req.product_id, 
                        packaging_hierarchy: `level${level}` 
                    },
                    data: { status: 'requested' }
                });
            } else if (product.thirdLayer_print && level === 3) {
                console.log(tableName, " does not exits ");
                await createDynamicTable(`${prodgen.generation_id.toLocaleLowerCase()}3_codes`, tx);
                await prisma.codeGenerationRequest.updateMany({ 
                    where: { 
                        batch_id: req.batch_id, 
                        product_id: 
                        req.product_id, 
                        packaging_hierarchy: `level${level}` 
                    },
                    data: { status: 'requested' }
                });
            }
          }
        });
      }

      // const codegenHistory = await prisma.$queryRawUnsafe(`
      //     SELECT batch_id, COUNT(*)
      //     FROM public."CodeGenerationRequestHistory"
      //     WHERE packaging_hierarchy = 'level${level}'
      //     GROUP BY batch_id
      //     HAVING COUNT(*) > 1
      //     ORDER BY batch_id DESC
      // `);

      // for (const req of codegenHistory){
      //     const codegenRequest = await prisma.$queryRawUnsafe(`
      //         SELECT
      //         batch_id,
      //         SUM(CAST(no_of_codes AS INTEGER)) AS total_requested_codes
      //         FROM public."CodeGenerationRequest"
      //         WHERE packaging_hierarchy = 'level${level}' and batch_id = '${req.batch_id}'
      //         GROUP BY batch_id
      //         ORDER BY batch_id DESC
      //     `);

      //     await prisma.codeGenerationRequestHistory.findMany({
      //         where: {
      //             packaging_hierarchy: `level${level}`,
      //             batch_id: req.batch_id
      //         },
      //         orderBy: { created_at: 'asc' }
      //     })
      // }
    }
  } catch (err) {
    console.error("❌ Error correction of batch tables:", err);
  } finally {
    await prisma.$disconnect();
  }
})();
