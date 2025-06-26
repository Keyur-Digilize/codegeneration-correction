import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

const gtinCodeLen = 13;
const lotSize = 1000;

const checkTableExists = async (tableName, tx = prisma) => {
  const result = await tx.$queryRaw`SELECT EXISTS (
           SELECT 1 
           FROM information_schema.tables 
           WHERE table_schema = 'public' 
           AND table_name = ${tableName}
         ) AS exists;`;

  return result[0]?.exists || false;
};

const calculateGtinCheckDigit = (input) => {
  let sum = 0;
  for (let i = 0; i < gtinCodeLen; i++) {
    const digit = parseInt(input[i]);
    sum += (i + 1) % 2 === 0 ? digit : digit * 3;
  }
  const nearestMultipleOfTen = Math.ceil(sum / 10) * 10;
  const checkDigit = nearestMultipleOfTen - sum;
  return checkDigit;
};

const createRecordsInDynamicTable = async (tableName, data, tx = prisma) => {
  const valuesPlaceholder = data
    .map(
      (_, index) =>
        `($${index * 7 + 1}::uuid, $${index * 7 + 2}::uuid, $${
          index * 7 + 3
        }::uuid, $${index * 7 + 4}, $${index * 7 + 5}, $${index * 7 + 6}, $${
          index * 7 + 7
        })`
    )
    .join(", ");

  const query = `
    INSERT INTO ${tableName}
    (product_id, batch_id, location_id, code_gen_id, unique_code, country_code, serial_no)
    VALUES ${valuesPlaceholder};
  `;

  const parameters = data.flatMap((item) => [
    item.product_id,
    item.batch_id,
    item.location_id,
    item.code_gen_id,
    item.unique_code,
    item.country_code,
    item.serial_no,
  ]);

  await tx.$executeRawUnsafe(query, ...parameters);
};

const getCountryCode = async ({
  codeStructure,
  ndc,
  gtin,
  batchNo,
  mfgDate,
  expDate,
  level,
  registration_no,
}) => {
  const elements =
    codeStructure.split("/").length > 1
      ? codeStructure.split("/")
      : codeStructure.split(" ");
  const finalCountryCode = [];
  for (const element of elements) {
    if (!element) {
      continue;
    }
    switch (element.trim()) {
      case "registrationNo":
        finalCountryCode.push(registration_no);
        break;

      case "NDC":
        finalCountryCode.push(ndc);
        break;

      case "GTIN": {
        const lastDigit = calculateGtinCheckDigit(`${level}${gtin}`);
        finalCountryCode.push(`${level}${gtin}${lastDigit}`);
        break;
      }

      case "batchNo":
        finalCountryCode.push(batchNo);
        break;

      case "manufacturingDate": {
        const date = new Date(mfgDate);
        const formattedDate = `${date
          .getFullYear()
          .toString()
          .slice(2)}${String(date.getMonth() + 1).padStart(2, "0")}${date
          .getDate()
          .toString()
          .padStart(2, "0")}`;
        finalCountryCode.push(formattedDate);
        break;
      }

      case "expiryDate": {
        const date = new Date(expDate);
        const formattedDate = `${date
          .getFullYear()
          .toString()
          .slice(2)}${String(date.getMonth() + 1).padStart(2, "0")}${date
          .getDate()
          .toString()
          .padStart(2, "0")}`;
        finalCountryCode.push(formattedDate);
        break;
      }

      case "<FNC>":
        finalCountryCode.push(String.fromCharCode(29));
        break;

      case "CRMURL":
        const superAdminConfigureData = await getSuperConfig();
        finalCountryCode.push(superAdminConfigureData.crm_url);
        break;

      default:
        finalCountryCode.push(element.trim());
        break;
    }
  }
  return codeStructure.split("/").length > 1
    ? finalCountryCode.join("/")
    : finalCountryCode.join("");
};

const insertInBulk = async (data) => {
  const countryCode = await getCountryCode({
    codeStructure: data.codeStructure,
    ndc: data.ndc,
    gtin: data.gtin,
    batchNo: data.batchNo,
    mfgDate: data.mfgDate,
    expDate: data.expDate,
    level: data.level,
  });
  // Prepare data array
  const codesData = data.codes.map((code) => {
    const uniqueId = `${data.generationId}${data.level}${code.code}`;
    return {
      serial_no: code.id,
      product_id: data.productId,
      batch_id: data.batchId,
      location_id: data.batchLocationId,
      code_gen_id: data.elementId,
      unique_code: uniqueId,
      country_code: countryCode.replaceAll("uniqueCode", uniqueId),
    };
  });
  // Bulk insert
  console.log("Inserting to db...");
  for (let i = 0; i < codesData.length; i += lotSize) {
    const chunk = codesData.slice(i, i + lotSize);
    await createRecordsInDynamicTable(data.tableName, chunk, data.tx);
  }
  console.log("Inserted to db");
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

const updateRecordInCodeSummary = async (data, tx = prisma) => {
  const packaging_hierarchy = data.packaging_hierarchy.replace("level", "");
  const previousSummaryOfCode = await tx.codeGenerationSummary.findFirst({
    where: {
      product_id: data.product_id,
      generation_id: data.generation_id,
      packaging_hierarchy: packaging_hierarchy,
    },
    select: {
      id: true,
      last_generated: true,
    },
  });
  if (previousSummaryOfCode) {
    const sumOfGenerated =
      data.generated +
      (previousSummaryOfCode?.last_generated
        ? parseInt(previousSummaryOfCode?.last_generated)
        : 0);
    await tx.codeGenerationSummary.update({
      where: { id: previousSummaryOfCode.id },
      data: {
        product_id: data.product_id,
        product_name: data.product_name,
        generation_id: data.generation_id,
        packaging_hierarchy: packaging_hierarchy,
        last_generated: String(sumOfGenerated),
      },
    });
  }
  // console.log("code summary updated", summary);
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
            select: {
              productNumber_print: true,
              firstLayer_print: true,
              secondLayer_print: true,
              thirdLayer_print: true,
            },
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
              const printed = await prisma.$queryRawUnsafe(
                `SELECT count(*) FROM "${tableName}" where batch_id = '${req.batch_id}' and printed = true`
              );

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
            } else if (int_total_requested_codes > int_total_generated_codes) {
              if (level === 0) return;
              if (level === 1) {
                if (req.batch_id === 'd56bcf97-c171-4013-94f3-eaee91ab1c09') return;
                console.log(
                  "Inserting data to dynamic table ",
                  int_total_requested_codes - int_total_generated_codes
                );
                const element = await prisma.codeGenerationRequest.findFirst({
                  where: {
                    product_id: req.product_id,
                    batch_id: req.batch_id,
                    packaging_hierarchy: "level1",
                  },
                  select: {
                    id: true,
                    product_id: true,
                    batch_id: true,
                    packaging_hierarchy: true,
                    no_of_codes: true,
                    generation_id: true,
                  },
                });
                console.log("element ", element);
                
                await prisma.$transaction(async (tx) => {
                  const product = await tx.product.findFirst({
                    where: { id: element.product_id },
                    select: {
                      id: true,
                      product_name: true,
                      prefix: true,
                      country_id: true,
                      ndc: true,
                      gtin: true,
                    },
                  });

                  const batch = await tx.batch.findFirst({
                    where: { id: element.batch_id },
                    select: {
                      id: true,
                      location_id: true,
                      producthistory_uuid: true,
                      batch_no: true,
                      manufacturing_date: true,
                      expiry_date: true,
                    },
                  });
                  const LEVEL = element.packaging_hierarchy.replace(
                    "level",
                    ""
                  );

                  const countryCodeStructure = await tx.countryMaster.findFirst(
                    {
                      where: { id: product.country_id },
                      select: { codeStructure: true },
                    }
                  );

                  const codeSummaryData = {
                    product_id: product.id,
                    product_name: product.product_name,
                    packaging_hierarchy: element.packaging_hierarchy,
                    generation_id: element.generation_id,
                  };

                  const insertBulkData = {
                    elementId: element.id,
                    generationId: element.generation_id,
                    codeStructure: countryCodeStructure.codeStructure,
                    productId: product.id,
                    ndc: product.ndc,
                    gtin: product.gtin,
                    batchId: batch.id,
                    batchNo: batch.batch_no,
                    mfgDate: batch.manufacturing_date,
                    expDate: batch.expiry_date,
                    batchLocationId: batch.location_id,
                    level: LEVEL,
                    tableName,
                    tx,
                  };

                  const skipped = await tx.codeGenerationSummary.findFirst({
                  where: {
                    product_id: element.product_id,
                    packaging_hierarchy: LEVEL,
                    generation_id: element.generation_id,
                  },
                  select: { last_generated: true },
                });

                console.log("last generated ", skipped, {
                  skip: skipped?.last_generated
                    ? parseInt(skipped?.last_generated)
                    : 0,
                  take: int_total_requested_codes - int_total_generated_codes,
                  orderBy: { id: "asc" },
                });
                
                const codes = await tx.codesGenerated.findMany({
                  skip: skipped?.last_generated
                    ? parseInt(skipped?.last_generated)
                    : 0,
                  take: int_total_requested_codes - int_total_generated_codes,
                  orderBy: { id: "asc" },
                });
                console.log("Codes take ", codes.length);
                
                await insertInBulk({ ...insertBulkData, codes });

                await updateRecordInCodeSummary(
                  {
                    ...codeSummaryData,
                    generated: codes.length,
                  },
                  tx
                );
                console.log("Done ", tableName);
                });
              }
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
                console.log(
                  `SSCC require not generated for batch ${req.batch_id}`
                );
              }
              return;
            }
            if (product.productNumber_print && level === 0) {
              console.log(tableName, " does not exits ");
              await createDynamicTable(
                `${prodgen.generation_id.toLocaleLowerCase()}0_codes`,
                tx
              );
            } else if (product.firstLayer_print && level === 1) {
              console.log(tableName, " does not exits ");
              await createDynamicTable(
                `${prodgen.generation_id.toLocaleLowerCase()}1_codes`,
                tx
              );
            } else if (product.secondLayer_print && level === 2) {
              console.log(tableName, " does not exits ");
              await createDynamicTable(
                `${prodgen.generation_id.toLocaleLowerCase()}2_codes`,
                tx
              );
            } else if (product.thirdLayer_print && level === 3) {
              console.log(tableName, " does not exits ");
              await createDynamicTable(
                `${prodgen.generation_id.toLocaleLowerCase()}3_codes`,
                tx
              );
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
