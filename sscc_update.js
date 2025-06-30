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

      await tx.$executeRawUnsafe(`UPDATE public.sscc_codes SET sscc_code = '389041349000087899' where sscc_code = '389041349000002724' and printed = false and is_aggregated = false and is_dropped = false`);

      await tx.$executeRawUnsafe(`
        WITH ids AS (
          SELECT id FROM public.gyxt1_codes where batch_id = '9fc67070-ae56-45a2-bce2-0fd78c0bac37'
          and unique_code in(
          'GYXT155AKUS6ESJ', 'GYXT133ZHJCL9V7', 'GYXT1MD1SUGWJL7', 'GYXT1X2NEVLTD69', 'GYXT17HVGIYBBP2', 'GYXT1NZBE0KJK35', 'GYXT1H8HB0QKDKK', 'GYXT1QN7SJN51B4', 'GYXT10KZG4F4A6T', 'GYXT1RNM8COCT9N', 'GYXT1VMYCIRWWII', 'GYXT1P4U87GUD60', 'GYXT1TSLKFRDDBY', 'GYXT1ZVHUAMUZ7H', 'GYXT1BJNVXYIC1N', 'GYXT1A64U9MRIUC', 'GYXT10EM472W7HI', 'GYXT17H8KFU8ZXQ', 'GYXT16U5SJD984V', 'GYXT14DPTM5ABE1', 'GYXT1NABLAAQVQ3', 'GYXT15EHMU43YN5', 'GYXT10N9K48LEQ1', 'GYXT1GMRI1ZFYP1', 'GYXT1ZU088MFFHY', 'GYXT1898NQDEL8I', 'GYXT18G9CFMQGR0', 'GYXT1RXE5U89P9S', 'GYXT1C2UIKD12JD', 'GYXT11SUNXM13HT', 'GYXT16SS2M072CY', 'GYXT1P5XFVFLLJH', 'GYXT1ZL4R56ZUHF', 'GYXT1DCLTY26CX8', 'GYXT1S0IL5E7G0Q', 'GYXT1K9NQQC0W9V', 'GYXT1CMS3LNOKRT', 'GYXT1DJV64FCACX', 'GYXT181QD4H0O9W', 'GYXT1YKAQ30ITGW', 'GYXT11MH2VVOHE1', 'GYXT1LYLZS89HYR', 'GYXT1WG6T2I08QV', 'GYXT1ZEI9PENCVF', 'GYXT1Q89I7LM7VV', 'GYXT1Q6B12QRBFN', 'GYXT1RMY51XJ8BQ', 'GYXT1JIOHU3ZSMC', 'GYXT1A8GXTNXR99', 'GYXT1W4G7DQ0785', 'GYXT1E34K9K420U', 'GYXT12VIIJA2X4G', 'GYXT1V81XQKNDRI', 'GYXT1CYY2Y39COV', 'GYXT11EAOD8X77H', 'GYXT1YAF1ZQTHO4', 'GYXT1JFNU37RPL8', 'GYXT1R0W39I6FAG', 'GYXT19MDEXE3R0X', 'GYXT1KVBQ486GE2', 'GYXT1ZIJF3FMG6O', 'GYXT1PD89P1FONZ', 'GYXT1U73PX1LHY4', 'GYXT19CB7DCQMH5', 'GYXT1PY4OGRUHM4', 'GYXT1TRQHIBD7QF', 'GYXT1BNTMBTL4MQ', 'GYXT1K1XX55G2BS', 'GYXT1A8ATAOF7ZT', 'GYXT1AC0OBYR90U', 'GYXT13YX3Y5DKD4', 'GYXT14WDIGKXCG0', 'GYXT1BBCM2A6QKZ', 'GYXT14HFSNSVYG1', 'GYXT1VJDQY7VE7D', 'GYXT1S9QSK8TROZ', 'GYXT11DZ6C8ITD2', 'GYXT1Y33X8F9CEP', 'GYXT1YHYMSSZKHI', 'GYXT1V0MCR80GVK', 'GYXT15M2HIEVUQH', 'GYXT1HNMIKHSSNB', 'GYXT1CDYDORV8ZZ', 'GYXT1455708DOVM', 'GYXT10Q77XJGAK6', 'GYXT1TCTUYK8SSG', 'GYXT1BAN8TGO9SJ', 'GYXT1EJ7HFA7FUF', 'GYXT1C4TEYZRHJ5', 'GYXT1ONQC4RA17L', 'GYXT1P8R83MF78B', 'GYXT1G2EGB5IFKL', 'GYXT16DPU2LTJYZ', 'GYXT1TENZMBOI5Z', 'GYXT1W2B212DL9A', 'GYXT12LEIOCGSU7', 'GYXT17JACOLRWC3', 'GYXT1E3HPJGIIA9', 'GYXT135DBJAW2I5', 'GYXT12CC3SXMWW2', 'GYXT1YFESZCTQ2C', 'GYXT1KO373YV2E2', 'GYXT1ARNSBH8T26', 'GYXT1URMDDFX82L', 'GYXT1J9Z9I59D9Y', 'GYXT1NSHL8SYEC5', 'GYXT1PA6WI3UQC0', 'GYXT1LLPT5Y4AUD', 'GYXT103TRGSUN1I', 'GYXT1G29RZX9DFI', 'GYXT15Y55KIL46O', 'GYXT14ETAZYSFW3', 'GYXT1YGDJO242QR', 'GYXT1Z75OR7KIWA', 'GYXT1YVHO3WWLV9', 'GYXT1E0YKQKO7EE', 'GYXT1ZLELVVBV8G', 'GYXT10XQBP1LRPJ', 'GYXT17BH0826GGJ', 'GYXT1AXVBD7O2JV', 'GYXT1AQLGK7YHX8', 'GYXT16467VM0XJV', 'GYXT1V4CSGHRDK1', 'GYXT1O7MPQRYNU4', 'GYXT1EJNS3QFM5Q', 'GYXT1CR9DNXNP6H', 'GYXT1A0GQXH3N06', 'GYXT1K4NIX15T3T', 'GYXT1JS7Y2X6FZC', 'GYXT1PWBYDWBF55', 'GYXT1FU878J204Q', 'GYXT101R04PHXJS', 'GYXT1BBAETQL4TL', 'GYXT1CHUSSVIKEA', 'GYXT179MVKFUJED', 'GYXT1TTIHWRT8EH', 'GYXT1E6F8K3JRSR', 'GYXT17IJRH2GVPT', 'GYXT1M02E6RDJB7', 'GYXT1VPPHBTTP2P', 'GYXT117DYU36HHT', 'GYXT1JNAJS1MAD3', 'GYXT1FITEN5TX61', 'GYXT13330IN9S6F', 'GYXT1MIDDU3AO5P', 'GYXT1GOLZLRG3QR', 'GYXT1EP473FE3YD', 'GYXT1GBG33B0AC3', 'GYXT1UGMX3OSX6H', 'GYXT1ILPFREXH8K', 'GYXT153IV5HWQ68', 'GYXT100HG2JTZ63', 'GYXT144PUECGRM3', 'GYXT1UOBE61IEKP', 'GYXT1GE7OXAWLAY', 'GYXT1BR1QHTHXI7', 'GYXT111ITSC10DK', 'GYXT1A7XHEI3NY6', 'GYXT1KB700GHMZG', 'GYXT18TDS0BBI38', 'GYXT1WR5L05AIDN', 'GYXT1AWI67BPLSQ', 'GYXT1BRFQHVGCZF', 'GYXT14VRCUSVH8B', 'GYXT1GFWO88KO8P', 'GYXT1Y1434M6GW3', 'GYXT1I4C17PE7S1', 'GYXT1MOCVC21O1K', 'GYXT120EJMR6EJ9', 'GYXT1X7KNUK4Y06', 'GYXT1WQNOD2L0PZ', 'GYXT1TCQTP2X8NR', 'GYXT1TQMNFV6VP5', 'GYXT1UAI66TXL40', 'GYXT17W0ZZGSM93', 'GYXT13AHWGJUL4I', 'GYXT1RBLBWCR14R', 'GYXT1KDJEGD8TKW', 'GYXT1DSXT4QL051', 'GYXT1BWJXV7RN5T', 'GYXT1YU23TQ55XE', 'GYXT13HFMJ9F1B1', 'GYXT1M0HOF2E86Y', 'GYXT1UG21HEVDP5', 'GYXT1JY2NGL6Z65', 'GYXT1PH56G1H9N8', 'GYXT1MCBTXTU4N3', 'GYXT1P7J4W4UN5O', 'GYXT1JIE4NJZWXE', 'GYXT1XJ0TU6TKGD', 'GYXT1EHIZTAF2FF', 'GYXT1S7AO6MDKC1', 'GYXT1690QWCEEPT', 'GYXT1IDIBU3Z5WO', 'GYXT17WTJ64AN2G', 'GYXT1A27LW6AS62', 'GYXT1IRIZ0O6RGT', 'GYXT1CRN4GGGCLJ', 'GYXT1UTFCD25TRN', 'GYXT1EB0KLPTKOD'
          )
        ) UPDATE public.gyxt1_codes SET parent_id = 'd0671c34-c9cc-402d-82d2-aa057c4478f2', is_aggregated = true WHERE id IN (SELECT id FROM ids)
      `)

      const s = await prisma.scanned_code.findMany({});
      await prisma.scanned_code.updateMany({ 
        where: {
          id: s[0].id,
          transaction_id: s[0].transaction_id
        },
        data: {
          scanned_1_codes: [s[0].scanned_1_codes[0]]
        }
      })
      await prisma.scanned_code.updateMany({ 
        where: {
          id: s[1].id,
          transaction_id: s[1].transaction_id
        },
        data: {
          scanned_1_codes: [{ '1' : s[1].scanned_1_codes[1]['2']}]
        }
      })
      console.log("🎉 All SSCC codes updated successfully.");
    }, { timeout: 600000 });
  } catch (err) {
    console.error("❌ Error updating SSCC codes:", err);
  } finally {
    await prisma.$disconnect();
  }
})();
