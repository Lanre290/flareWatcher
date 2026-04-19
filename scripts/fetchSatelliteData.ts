import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import * as turf from '@turf/turf';

type OilBlockProperties = {
  block: string;
  operator: string;
};

type Attribution = {
  block: string;
  operator: string;
  trend: string;
};

type CsvRow = {
  id: string;
  Lat: string;
  Lon: string;
  rad_heat: string;
  temp_bg: string;
};

type TelemetryPoint = {
  id: string;
  lat: number;
  lng: number;
  radiant_heat_mscf: number;
  attribution: Attribution | null;
};

type FinalPayload = {
  meta: {
    satellite: string;
    dataset: string;
    timestamp: string;
    region: string;
  };
  telemetry: TelemetryPoint[];
};

// 1. DUMMY GEOJSON FOR ATTRIBUTION 
// FIX: Using Turf's built-in functions to generate the GeoJSON fixes all TypeScript errors automatically.
const oilBlocksGeoJSON = turf.featureCollection([
  turf.polygon(
    [[[6.9, 4.7], [7.2, 4.7], [7.2, 4.9], [6.9, 4.9], [6.9, 4.7]]], 
    { block: "OML 17", operator: "NNPC" } as OilBlockProperties
  )
  // Add more African blocks here...
]);

const AFRICA_BOUNDS = {
  minLat: -35,
  maxLat: 37,
  minLng: -17,
  maxLng: 51
} as const;

function parseNumber(value: string): number | null {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isWithinAfricaBounds(lat: number, lng: number): boolean {
  return (
    lat >= AFRICA_BOUNDS.minLat &&
    lat <= AFRICA_BOUNDS.maxLat &&
    lng >= AFRICA_BOUNDS.minLng &&
    lng <= AFRICA_BOUNDS.maxLng
  );
}

async function runPipeline() {
  console.log("🚀 BOOTING SATELLITE PIPELINE...");

  try {
    // For this example, we'll simulate downloading the CSV data
    const mockCsvData = `id,Lat,Lon,rad_heat,temp_bg
V1,4.8156,7.0498,45.2,290
V2,31.6000,6.0000,38.1,285
V3,-6.3000,12.5000,35.5,295
V4,45.0000,-100.0000,50.0,280`; // V4 is in USA, should be filtered out

    // STEP 2: PARSE CSV
    console.log("📊 Parsing Global Telemetry...");
    const records = parse(mockCsvData, {
      columns: true,
      skip_empty_lines: true
    }) as CsvRow[];

    // STEP 3: FILTER FOR AFRICA & CONVERT DATA
    console.log("🌍 Filtering for Africa Bounding Box...");
    const africanFlares: TelemetryPoint[] = [];
    
    records.forEach((row) => {
      const lat = parseNumber(row.Lat);
      const lng = parseNumber(row.Lon);
      const radiantHeatMW = parseNumber(row.rad_heat);

      if (lat === null || lng === null || radiantHeatMW === null) {
        return;
      }

      // Africa Bounding Box
      if (isWithinAfricaBounds(lat, lng)) {
        
        const estimatedMscf = radiantHeatMW * 2.48; 

        // STEP 4: GEOSPATIAL ATTRIBUTION (The Magic)
        const flarePoint = turf.point([lng, lat]);
        let attribution: Attribution | null = null;

        // FIX: No more TS type casting needed here!
        for (const block of oilBlocksGeoJSON.features) {
          if (turf.booleanPointInPolygon(flarePoint, block)) {
            attribution = {
              block: block.properties.block,
              operator: block.properties.operator,
              trend: "N/A" 
            };
            break; 
          }
        }

        africanFlares.push({
          id: row.id,
          lat,
          lng,
          radiant_heat_mscf: estimatedMscf,
          attribution
        });
      }
    });

    // STEP 5: COMPILE FINAL JSON PAYLOAD
    const finalPayload: FinalPayload = {
      meta: {
        satellite: "Suomi NPP / VIIRS",
        dataset: "VNF_Nightfire",
        timestamp: new Date().toISOString(),
        region: "AFRICA_CONTINENT"
      },
      telemetry: africanFlares
    };

    // STEP 6: SAVE TO DISK
    const outputPath = path.join(process.cwd(), 'public', 'latest_flares.json');
    
    // Safety check: ensure public directory exists before writing
    if (!fs.existsSync(path.dirname(outputPath))) {
      fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    }

    fs.writeFileSync(outputPath, JSON.stringify(finalPayload, null, 2));
    
    console.log(`✅ PIPELINE COMPLETE. Saved ${africanFlares.length} active flares to disk.`);

  } catch (error) {
    console.error("❌ PIPELINE FAILED:", error);
  }
}

runPipeline();