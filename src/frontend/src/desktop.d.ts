export type RuntimeConfig = {
  backendBaseUrl: string;
  backendPort: number;
  providerName: string;
} | null;

declare global {
  interface Window {
    desktop?: {
      getRuntimeConfig: () => Promise<RuntimeConfig>;
    };
  }
}

export {};
