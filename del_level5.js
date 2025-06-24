import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

(async () => {
  try {
    await prisma.$transaction(async (tx) => {
        const products = await tx.productGenerationId.findMany({ select: { generation_id: true }});
        for (const element of products) {
            await tx.$executeRawUnsafe(`DROP TABLE IF EXISTS "${element.generation_id.toLocaleLowerCase()}5_codes"`);
        }
        console.log("✅ Delete all level 5 tables");
        
    });
  } catch (err) {
    console.error("❌ Error delete level 5 tables:", err);
  } finally {
    await prisma.$disconnect();
  }
})();
