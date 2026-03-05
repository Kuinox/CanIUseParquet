import fs from "fs";
import path from "path";
import { MatrixData } from "../types/matrix";

export function getMatrixData(): MatrixData {
  const filePath = path.join(process.cwd(), "public", "data", "matrix.json");
  try {
    const data = fs.readFileSync(filePath, "utf-8");
    return JSON.parse(data) as MatrixData;
  } catch (err) {
    throw new Error(
      `Failed to load matrix data from ${filePath}: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}
