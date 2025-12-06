import { Client } from 'pg';

/**
 * Script to generate geographic slugs for existing parks
 *
 * Run with: npx ts-node scripts/generate-geographic-slugs.ts
 */

// Simple slug generator (copied from slug.util.ts to avoid import issues)
function generateSlug(text: string): string {
  const slugify = require('slugify');
  const { transliterate } = require('transliteration');

  const transliterated = transliterate(text);
  return slugify(transliterated, {
    lower: true,
    strict: true,
    trim: true,
  });
}

async function generateGeographicSlugs() {
  const client = new Client({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    user: process.env.DB_USER || 'parkfan',
    password: process.env.DB_PASSWORD || 'parkfan_dev_password',
    database: process.env.DB_NAME || 'parkfan',
  });

  try {
    console.log('🔌 Connecting to database...');
    await client.connect();
    console.log('✅ Connected to database');

    // Find all parks with geographic data but missing slugs
    const result = await client.query(`
      SELECT id, name, continent, "continentSlug", country, "countrySlug", city, "citySlug"
      FROM parks
      WHERE (continent IS NOT NULL AND "continentSlug" IS NULL)
         OR (country IS NOT NULL AND "countrySlug" IS NULL)
         OR (city IS NOT NULL AND "citySlug" IS NULL)
    `);

    console.log(`📊 Found ${result.rows.length} parks to update`);

    let updated = 0;
    for (const park of result.rows) {
      const updates: string[] = [];
      const values: string[] = [];
      let valueIndex = 1;

      // Generate continent slug
      if (park.continent && !park.continentSlug) {
        updates.push(`"continentSlug" = $${valueIndex++}`);
        values.push(generateSlug(park.continent));
      }

      // Generate country slug
      if (park.country && !park.countrySlug) {
        updates.push(`"countrySlug" = $${valueIndex++}`);
        values.push(generateSlug(park.country));
      }

      // Generate city slug
      if (park.city && !park.citySlug) {
        updates.push(`"citySlug" = $${valueIndex++}`);
        values.push(generateSlug(park.city));
      }

      if (updates.length > 0) {
        values.push(park.id);
        await client.query(
          `UPDATE parks SET ${updates.join(', ')} WHERE id = $${valueIndex}`,
          values
        );
        updated++;

        const slugs = [
          park.continent ? generateSlug(park.continent) : null,
          park.country ? generateSlug(park.country) : null,
          park.city ? generateSlug(park.city) : null,
        ].filter(Boolean).join('/');

        console.log(`  ✓ Updated: ${park.name} (${slugs})`);
      }
    }

    console.log(`\n✅ Successfully updated ${updated} parks`);
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  } finally {
    await client.end();
    console.log('🔌 Database connection closed');
  }
}

generateGeographicSlugs();
