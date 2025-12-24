import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { AdminController } from "./admin.controller";

@Module({
    imports: [
        BullModule.registerQueue({ name: "holidays" }),
        BullModule.registerQueue({ name: "park-metadata" }),
    ],
    controllers: [AdminController],
})
export class AdminModule { }
