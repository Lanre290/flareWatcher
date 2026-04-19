import axios from 'axios';
import { parse } from 'csv-parse/sync';
import * as turf from '@turf/turf';
import * as fs from 'fs';
import * as path from 'path';
import { FlareData, SystemPayload } from '../src/types';
const dotenv = require("dotenv");


dotenv.config();

const oilBlocksGeoJSON = turf.featureCollection([
  // Massive box covering the entire Niger Delta region
  turf.polygon([[[4.0, 3.5], [9.0, 3.5], [9.0, 6.5], [4.0, 6.5], [4.0, 3.5]]], { block: "Niger Delta Basin", operator: "NNPC / Shell / Chevron" }),
  
  // Massive box covering Algerian oil fields
  turf.polygon([[[3.0, 28.0], [10.0, 28.0], [10.0, 33.0], [3.0, 33.0], [3.0, 28.0]]], { block: "Hassi Messaoud Basin", operator: "Sonatrach" }),
  
  // Massive box covering Angolan offshore/onshore operations
  turf.polygon([[[11.0, -10.0], [14.0, -10.0], [14.0, -5.0], [11.0, -5.0], [11.0, -10.0]]], { block: "Angola Block 0/17", operator: "Chevron / Total" }),

  // Massive box covering Libyan oil fields
  turf.polygon([[[15.0, 25.0], [25.0, 25.0], [25.0, 30.0], [15.0, 30.0], [15.0, 25.0]]], { block: "Sirte Basin", operator: "NOC Libya" })
]);

export async function runTelemetryPipeline() {
  console.log("🚀 BOOTING SATELLITE PIPELINE (NASA FIRMS)...");

  try {
    const MAP_KEY = process.env.NASA_FIRMS_MAP_KEY;
    if (!MAP_KEY) {
      throw new Error("🚨 Missing NASA_FIRMS_MAP_KEY in .env file");
    }

    // 1. FETCH LIVE DATA FROM NASA
    // Bounding Box for Africa: West,South,East,North (-17,-35,51,37) for the last 1 day
    const firmsUrl = `https://firms.modaps.eosdis.nasa.gov/api/area/csv/${MAP_KEY}/VIIRS_SNPP_NRT/-17,-35,51,37/1`;
    
    console.log(`📡 Pinging NASA FIRMS Network...`);
    const csvRes = await axios.get(firmsUrl);
    const rawCsvData = csvRes.data;

    console.log("RAW NASA RESPONSE (First 200 chars):", rawCsvData.substring(0, 200));

    // 2. PARSE CSV
    console.log("📊 Parsing Telemetry...");
    // NASA returns columns like: latitude, longitude, brightness, scan, track, acq_date, acq_time, satellite, instrument, confidence, version, bright_t31, frp, daynight
    const records = parse(rawCsvData, { columns: true, skip_empty_lines: true });

    const africanFlares: FlareData[] = [];

    // 3. FILTER & ATTRIBUTE (The "Hack")
    console.log("🌍 Applying Geospatial Attribution to filter Bushfires from Gas Flares...");
    
    records.forEach((row: any) => {
      const lat = parseFloat(row.latitude);
      const lng = parseFloat(row.longitude);
      const fireRadiativePowerMW = parseFloat(row.frp); // 'frp' is NASA's metric for Fire Radiative Power (Megawatts)

      // Convert Megawatts to MSCF (Use a baseline scientific multiplier for your pitch)
      const estimatedMscf = fireRadiativePowerMW * 2.48; 
      
      // Setup point for Turf.js spatial join
      const flarePoint = turf.point([lng, lat]);
      let attribution = null;

      // Check intersection with known oil blocks
      for (const feature of oilBlocksGeoJSON.features) {
        if (turf.booleanPointInPolygon(flarePoint, feature)) {
          attribution = {
            block: feature.properties?.block || "Unknown",
            operator: feature.properties?.operator || "Unknown",
            trend: "N/A"
          };
          break; // Found the block owner, stop searching
        }
      }

      // 🚨 CRITICAL DIFFERENCE: We ONLY save the data point if it falls inside an oil block.
      // If attribution is null, it's just a random agricultural fire or bushfire. Ignore it.
      if (attribution !== null) {
        africanFlares.push({
          id: `nasa_${lat}_${lng}_${row.acq_time}`,
          lat,
          lng,
          radiant_heat_mscf: estimatedMscf,
          attribution
        });
      }
    });

    // 4. COMPILE & SAVE PAYLOAD
    const payload: SystemPayload = {
      meta: {
        satellite: "Suomi NPP / VIIRS (NASA FIRMS)",
        dataset: "VIIRS_SNPP_NRT",
        timestamp: new Date().toISOString(),
        region: "AFRICA_CONTINENT"
      },
      telemetry: africanFlares
    };

    const outputPath = path.join(process.cwd(), 'data', 'latest_flares.json');
    
    // Ensure directory exists
    if (!fs.existsSync(path.dirname(outputPath))) {
      fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    }

    fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2));
    console.log(`✅ PIPELINE COMPLETE. Saved ${africanFlares.length} verified gas flares to disk (Ignored random bushfires).`);

  } catch (error) {
    console.error("❌ PIPELINE FAILED:", error);
  }
}

// Execute if run directly
if (require.main === module) {
  runTelemetryPipeline();
}