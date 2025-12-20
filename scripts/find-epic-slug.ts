
import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { DataSource } from "typeorm";

async function run() {
    const app = await NestFactory.createApplicationContext(AppModule);
    const dataSource = app.get(DataSource);

    const park = await dataSource.query(`SELECT id, name, slug FROM parks WHERE name ILIKE '%Epic Universe%'`);
    console.log(JSON.stringify(park, null, 2));

    await app.close();
}

run();
