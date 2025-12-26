import { ApiProperty } from "@nestjs/swagger";

export class BreadcrumbDto {
    @ApiProperty({ description: "Display name", example: "Europe" })
    name: string;

    @ApiProperty({ description: "URL path", example: "/europe" })
    url: string;
}
