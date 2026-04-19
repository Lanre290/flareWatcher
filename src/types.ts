// types.ts
export interface EOGDailyResponse {
  name: string;
  url: string;
  size: number;
}

export interface FlareData {
  id: string;
  lat: number;
  lng: number;
  radiant_heat_mscf: number;
  attribution: FlareAttribution | null;
}

export interface FlareAttribution {
  block: string;
  operator: string;
  trend: string;
}

export interface SystemPayload {
  meta: {
    satellite: string;
    dataset: string;
    timestamp: string;
    region: string;
  };
  telemetry: FlareData[];
}