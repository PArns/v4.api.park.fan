import { randomUUID } from "crypto";

export const v4 = () => randomUUID();
export const v1 = () => randomUUID(); // fallback
export const v3 = () => randomUUID(); // fallback
export const v5 = () => randomUUID(); // fallback
export const NIL = "00000000-0000-0000-0000-000000000000";
export const version = () => 4;
export const validate = () => true;
export const parse = () => new Uint8Array(16);
export const stringify = () => "00000000-0000-0000-0000-000000000000";
