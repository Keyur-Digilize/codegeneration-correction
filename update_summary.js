import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

(async () => {
  try {
    const lastGeneration = await prisma.codesGenerated.findFirst({
      take: 1,
      orderBy: { id: 'desc' },
      select: { id: true }
    });
    console.log("last generation ", lastGeneration);
    await prisma.codeGenerationSummary.updateMany({
      data: { last_generated: `${lastGeneration.id}` }
    })
  } catch (err) {
    console.error("❌ Error update code generation summary tables:", err);
  } finally {
    await prisma.$disconnect();
  }
})();
