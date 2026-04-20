import { DataSource } from "typeorm";
import { QueueData } from "./src/queue-data/entities/queue-data.entity";
import { Attraction } from "./src/attractions/entities/attraction.entity";
import { Park } from "./src/parks/entities/park.entity";
import * as dotenv from "dotenv";

dotenv.config({ path: ".env.production.example" }); // Use test env if needed, or we just look at the code

console.log("SQL test script created.");
