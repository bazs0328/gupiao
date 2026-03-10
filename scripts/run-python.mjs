import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

function isWsl() {
  if (process.platform !== "linux") {
    return false;
  }
  if (process.env.WSL_DISTRO_NAME) {
    return true;
  }
  try {
    return fs.readFileSync("/proc/version", "utf8").toLowerCase().includes("microsoft");
  } catch {
    return false;
  }
}

function probe(command, args) {
  const result = spawnSync(command, [...args, "--version"], { stdio: "ignore" });
  return result.status === 0;
}

function toWindowsPath(value) {
  const absolute = fs.existsSync(path.resolve(value)) ? path.resolve(value) : value;
  if (!absolute.startsWith("/")) {
    return value;
  }
  const result = spawnSync("wslpath", ["-w", absolute], { encoding: "utf8" });
  if (result.status !== 0) {
    return value;
  }
  return result.stdout.trim();
}

function run(command, args) {
  const child = spawnSync(command, args, { stdio: "inherit" });
  if (child.error) {
    throw child.error;
  }
  process.exit(child.status ?? 1);
}

const rawArgs = process.argv.slice(2);
const preferWindows = rawArgs[0] === "--prefer-windows";
const args = preferWindows ? rawArgs.slice(1) : rawArgs;

if (args.length === 0) {
  console.error("Usage: node scripts/run-python.mjs [--prefer-windows] <python args>");
  process.exit(1);
}

if (preferWindows && process.platform === "win32") {
  if (probe("py", ["-3"])) {
    run("py", ["-3", ...args]);
  }
  run("python", args);
}

if (preferWindows && isWsl()) {
  const convertedArgs = args.map((value) => {
    if (value.startsWith("-")) {
      return value;
    }
    if (fs.existsSync(path.resolve(value))) {
      return toWindowsPath(value);
    }
    return value;
  });
  if (probe("cmd.exe", ["/c", "py", "-3"])) {
    run("cmd.exe", ["/c", "py", "-3", ...convertedArgs]);
  }
  run("cmd.exe", ["/c", "python", ...convertedArgs]);
}

const candidates =
  process.platform === "win32"
    ? [
        ["py", ["-3"]],
        ["python", []],
      ]
    : [
        ["python3", []],
        ["python", []],
      ];

for (const [command, commandArgs] of candidates) {
  if (probe(command, commandArgs)) {
    run(command, [...commandArgs, ...args]);
  }
}

console.error("No Python interpreter found.");
process.exit(1);
