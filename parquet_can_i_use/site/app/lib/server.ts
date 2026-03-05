import fs from "fs";
import path from "path";
import { MatrixData } from "../types/matrix";

export function getMatrixData(): MatrixData {
  const filePath = path.join(process.cwd(), "public", "data", "matrix.json");
  const data = fs.readFileSync(filePath, "utf-8");
  return JSON.parse(data);
}
