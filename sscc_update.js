import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

const ssccLenCheckSum = 17;
const EXTENSION_DIGIT = 3;

const calculateSsccCheckDigit = (input) => {
  let sum = 0;
  for (let i = 0; i < ssccLenCheckSum; i++) {
    const digit = parseInt(input[i]);
    sum += (i + 1) % 2 === 0 ? digit : digit * 3;
  }
  const nearestMultipleOfTen = Math.ceil(sum / 10) * 10;
  const checkDigit = nearestMultipleOfTen - sum;
  return checkDigit;
};

(async () => {
  try {
    await prisma.$transaction(async (tx) => {
        const prefix = "89041349";
      const rawIds = await tx.$queryRawUnsafe(
        `SELECT * FROM public.sscc_codes WHERE is_aggregated = false and printed = false and is_dropped = false and sscc_code LIKE '389041349%' 
        ORDER BY created_at, product_id, batch_id, pack_level`
      );

      const ids = rawIds.map(row => row.id);
      console.log("IDs count:", ids.length);

      const ssccCodes = [];
      for (let i = 1; i <= ids.length; i++) {
        const SIXTEEN_CHAR = (
          parseInt(prefix.padEnd(16, "0")) + i
        ).toString();
        const checkDigit = calculateSsccCheckDigit(`${EXTENSION_DIGIT}${SIXTEEN_CHAR}`);
        const fullSSCC = `${EXTENSION_DIGIT}${SIXTEEN_CHAR}${checkDigit}`;
        ssccCodes.push(fullSSCC);
      }

      if (ids.length !== ssccCodes.length) {
        throw new Error("Mismatch between IDs and SSCC codes.");
      }

      for (let i = 0; i < ids.length; i++) {
        await tx.$executeRawUnsafe(
          `UPDATE public.sscc_codes SET sscc_code = $1 WHERE id = $2::uuid`,
          ssccCodes[i],
          ids[i]
        );
        console.log(`✅ Updated: ID ${ids[i]} -> ${ssccCodes[i]}`);
      }

      console.log("🎉 All SSCC codes updated successfully.");
    }, { timeout: 600000 });
  } catch (err) {
    console.error("❌ Error updating SSCC codes:", err);
  } finally {
    await prisma.$disconnect();
  }
})();
