import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

const createRecordsInSsccCodes = async (data, tx = prisma) => {
  // Prepare the query for bulk insert
  const valuesPlaceholder = data
    .map(
      (_, index) =>
        `($${index * 7 + 1}, $${index * 7 + 2}, $${index * 7 + 3}::uuid, $${
          index * 7 + 4
        }::uuid, $${index * 7 + 5}::uuid, $${index * 7 + 6}::uuid, $${
          index * 7 + 7
        }::uuid)`
    )
    .join(", ");

  const query = `
        INSERT INTO "sscc_codes"
        (sscc_code, pack_level, product_id, batch_id, product_history_id, location_id, code_gen_id)
        VALUES ${valuesPlaceholder}
    `;

  // Flatten the data array into a single array for parameterized query
  const parameters = data.flat();

  // Execute the query
  await tx.$executeRawUnsafe(query, ...parameters);
};

(async () => {
  try {
    const originalData = await prisma.$queryRawUnsafe(`
      SELECT 
        cgr_summary.batch_id,
        cgr_summary.total_requested_codes,
        COALESCE(sc_summary.total_generated_codes, 0) AS total_generated_codes
      FROM (
        SELECT 
          batch_id,
          SUM(CAST(no_of_codes AS INTEGER)) AS total_requested_codes
        FROM public."CodeGenerationRequest"
        WHERE packaging_hierarchy = 'level5'
        GROUP BY batch_id
      ) cgr_summary
      LEFT JOIN (
        SELECT 
          batch_id,
          COUNT(*) AS total_generated_codes
        FROM public."sscc_codes"
        GROUP BY batch_id
      ) sc_summary
      ON cgr_summary.batch_id = sc_summary.batch_id
      ORDER BY cgr_summary.total_requested_codes DESC
    `);
    const cleanedData = originalData.map((item) => ({
      batch_id: item.batch_id,
      total_requested_codes: Number(item.total_requested_codes),
      total_generated_codes: Number(item.total_generated_codes),
    }));

    for (const element of cleanedData) {
      const codes = [];
      const codegen = await prisma.codeGenerationRequest.findFirst({
        where: { batch_id: element.batch_id },
        select: { id: true, product_id: true, location_id: true },
      });
      const b = await prisma.batch.findFirst({
        where: { id: element.batch_id },
        select: { producthistory_uuid: true },
      });
      const diffCount =
        element.total_requested_codes - element.total_generated_codes;
      for (let i = 1; i <= diffCount; i++) {
        codes.push([
          "389041349000009999",
          5,
          codegen.product_id, // product id
          element.batch_id, // batch id
          b.producthistory_uuid, // product history id
          codegen.location_id, // location id
          codegen.id, // code generation id
        ]);
      }

      if (codes.length > 0) {
        await prisma.$transaction(async (tx) => {
          await createRecordsInSsccCodes(codes, tx);
        });
      } else {
        console.log(`Skipping batch ${element.batch_id} — no codes to insert.`);
      }
    }
  } catch (err) {
    console.error("❌ Error add SSCC codes:", err);
  } finally {
    await prisma.$disconnect();
  }
})();
