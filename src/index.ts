import express, { Request, Response } from 'express';
import * as fs from 'fs';
import * as path from 'path';
import cron from 'node-cron';

// Import your pipeline!
import { runTelemetryPipeline } from './../services/pipeline';

const app = express();
const PORT = process.env.PORT || 8080;
const DATA_PATH = path.join(__dirname, '..', 'public', 'latest_flares.json');

// CORS setup
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// --- AUTOMATION MAGIC ---

// 1. Schedule the pipeline to run automatically (e.g., every 12 hours)
// "0 */12 * * *" means minute 0, past every 12th hour.
cron.schedule('0 */12 * * *', async () => {
  console.log('⏰ CRON TRIGGERED: Running scheduled telemetry update...');
  await runTelemetryPipeline();
});

// 2. The Cold Start Fix: If the file is completely missing on boot, run it immediately
const ensureDataExists = async () => {
  if (!fs.existsSync(DATA_PATH)) {
    console.log('⚠️ INITIAL BOOT: No telemetry data found. Running pipeline immediately...');
    await runTelemetryPipeline();
  } else {
    console.log('✅ BOOT: Telemetry data found on disk. Ready to serve.');
  }
};

// --- ENDPOINTS ---

app.get('/api/v1/telemetry/live', (req: Request, res: Response) => {
  try {
    if (!fs.existsSync(DATA_PATH)) {
      // If it's still missing, the pipeline is likely still running its initial boot
      return res.status(503).json({ 
        error: "Telemetry data is currently compiling. Please retry in 10 seconds." 
      });
    }

    const fileData = fs.readFileSync(DATA_PATH, 'utf-8');
    const payload = JSON.parse(fileData);

    res.setHeader('Cache-Control', 'public, max-age=3600');
    return res.status(200).json(payload);
    
  } catch (error) {
    console.error("Endpoint Error:", error);
    return res.status(500).json({ error: "Internal Server Error" });
  }
});

// --- BOOT SEQUENCE ---

app.listen(PORT, async () => {
  console.log(`🟢 FlareWatch API running on port ${PORT}`);
  
  // Kick off the data check as soon as the server turns on
  await ensureDataExists();
});