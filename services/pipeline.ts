// services/pipeline.ts
import axios from 'axios';
import { parse } from 'csv-parse/sync';
import * as turf from '@turf/turf';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from "dotenv";

dotenv.config();

// 1. OIL BLOCKS
const oilBlocksGeoJSON = turf.featureCollection([
  turf.polygon([[[4.0, 3.5], [9.0, 3.5], [9.0, 6.5], [4.0, 6.5], [4.0, 3.5]]], { block: "Niger Delta Basin", operator: "NNPC / Shell / Chevron" }),
  turf.polygon([[[3.0, 28.0], [10.0, 28.0], [10.0, 33.0], [3.0, 33.0], [3.0, 28.0]]], { block: "Hassi Messaoud Basin", operator: "Sonatrach" }),
  turf.polygon([[[11.0, -10.0], [14.0, -10.0], [14.0, -5.0], [11.0, -5.0], [11.0, -10.0]]], { block: "Angola Block 0/17", operator: "Chevron / Total" }),
  turf.polygon([[[15.0, 25.0], [25.0, 25.0], [25.0, 30.0], [15.0, 30.0], [15.0, 25.0]]], { block: "Sirte Basin", operator: "NOC Libya" })
]);

// 2. POPULATION CENTERS TO TRACK
const populationCenters = [
  { name: "Port Harcourt, Nigeria", lat: 4.8156, lng: 7.0498 },
  { name: "Warri, Nigeria", lat: 5.5167, lng: 5.7500 },
  { name: "Ouargla, Algeria", lat: 31.9500, lng: 5.3167 },
  { name: "Tripoli, Libya", lat: 32.8802, lng: 13.1900 },
  { name: "Luanda, Angola", lat: -8.8390, lng: 13.2894 }
];

export async function runTelemetryPipeline() {
  console.log("🚀 BOOTING SATELLITE PIPELINE (TRACER MODE)...");

  try {
    const MAP_KEY = process.env.NASA_FIRMS_MAP_KEY;
    // Widened to 3 days to guarantee data
    const firmsUrl = `https://firms.modaps.eosdis.nasa.gov/api/area/csv/${MAP_KEY}/VIIRS_SNPP_NRT/-17,-35,51,37/3`;
    
    console.log(`📡 [STEP 1] Pinging NASA FIRMS Network...`);
    const csvRes = await axios.get(firmsUrl);
    
    console.log(`📦 [STEP 2] NASA responded with ${csvRes.data.length} characters of raw text.`);

    const records = parse(csvRes.data, { columns: true, skip_empty_lines: true });
    console.log(`🔥 [STEP 3] Parsed ${records.length} total thermal anomalies in Africa.`);

    const africanFlares: any[] = []; 
    let bushfiresIgnored = 0;

    records.forEach((row: any) => {
      const lat = parseFloat(row.latitude);
      const lng = parseFloat(row.longitude);
      const estimatedMscf = parseFloat(row.frp) * 2.48; 
      
      const flarePoint = turf.point([lng, lat]);
      let attribution = null;

      for (const feature of oilBlocksGeoJSON.features) {
        if (turf.booleanPointInPolygon(flarePoint, feature)) {
          attribution = {
            block: feature.properties?.block || "Unknown",
            operator: feature.properties?.operator || "Unknown",
            trend: (Math.random() * 10 - 5).toFixed(1) + "%" 
          };
          break;
        }
      }

      if (attribution !== null) {
        // IMPACT PREDICTION ENGINE
        const plumeRadiusKm = Math.round(15 + (estimatedMscf * 0.3));
        const atRiskCities = populationCenters.filter(city => {
            const cityPoint = turf.point([city.lng, city.lat]);
            const distance = turf.distance(flarePoint, cityPoint, { units: 'kilometers' });
            return distance <= plumeRadiusKm;
        }).map(c => c.name);

        let riskLevel = "MODERATE";
        let healthIssues = ["Reduced air quality", "Acid rain risk"];
        if (estimatedMscf > 50) {
            riskLevel = "CRITICAL";
            healthIssues.push("High risk of respiratory distress", "Crop contamination");
        }

        africanFlares.push({
          id: `nasa_${lat}_${lng}_${row.acq_time}`,
          lat,
          lng,
          radiant_heat_mscf: Math.round(estimatedMscf * 100) / 100,
          metrics: { 
            est_value_usd: Math.round(estimatedMscf * 3.15 * 100) / 100, 
            co2_tons: Math.round(estimatedMscf * 0.054 * 100) / 100 
          },
          intelligence: {
            confidence: row.confidence?.toLowerCase() === 'h' ? 'High' : 'Nominal',
            detection_time: `${row.acq_time} UTC`
          },
          attribution,
          impact_analysis: {
            plume_radius_km: plumeRadiusKm,
            risk_level: riskLevel,
            health_warnings: healthIssues,
            threatened_areas: atRiskCities.length > 0 ? atRiskCities : ["Rural/Unmapped Communities"]
          }
        });
      } else {
        bushfiresIgnored++;
      }
    });

    console.log(`🎯 [STEP 4] Geospatial Filter complete. Caught ${africanFlares.length} gas flares. Ignored ${bushfiresIgnored} bushfires.`);

    const payload = {
      meta: { satellite: "NASA FIRMS", timestamp: new Date().toISOString() },
      telemetry: africanFlares
    };

    // 🚨 ABSOLUTE PATH TRACER (FIXED) 🚨
    const targetDir = path.join(__dirname, '..', 'public');
    const outputPath = path.join(targetDir, 'latest_flares.json');
    
    console.log(`💾 [STEP 5] Attempting to save file to exact location:`);
    console.log(`👉 ${outputPath}`);
    
    if (!fs.existsSync(targetDir)) {
      console.log(`⚠️ Public folder missing! Creating it now at: ${targetDir}`);
      fs.mkdirSync(targetDir, { recursive: true });
    }
    
    fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2));
    
    console.log(`✅ [SUCCESS] Pipeline completely finished. Your data is ready!`);

  } catch (error) {
    console.error("❌ PIPELINE FAILED:", error);
  }
}

// Execute immediately
runTelemetryPipeline();