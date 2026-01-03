import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as request from 'supertest';
import { AppModule } from '../../src/app.module';

describe('Park Analytics (e2e)', () => {
    let app: INestApplication;

    beforeAll(async () => {
        const moduleFixture: TestingModule = await Test.createTestingModule({
            imports: [AppModule],
        }).compile();

        app = moduleFixture.createNestApplication();
        await app.init();
    });

    afterAll(async () => {
        await app.close();
    });

    describe('GET /v1/parks/:continent/:country/:city/:slug', () => {
        it('should return park with non-null analytics object', () => {
            return request(app.getHttpServer())
                .get('/v1/parks/europe/germany/bruehl/phantasialand')
                .expect(200)
                .expect((res) => {
                    expect(res.body).toBeDefined();
                    expect(res.body.analytics).toBeDefined();
                    expect(res.body.analytics).not.toBeNull();

                    // Analytics should have required structure even if park is closed
                    expect(res.body.analytics).toHaveProperty('occupancy');
                    expect(res.body.analytics).toHaveProperty('statistics');

                    // Statistics should have required fields
                    const stats = res.body.analytics.statistics;
                    expect(stats).toHaveProperty('avgWaitTime');
                    expect(stats).toHaveProperty('avgWaitToday');
                    expect(stats).toHaveProperty('peakWaitToday');
                    expect(stats).toHaveProperty('crowdLevel');
                    expect(stats).toHaveProperty('totalAttractions');
                    expect(stats).toHaveProperty('operatingAttractions');
                    expect(stats).toHaveProperty('closedAttractions');

                    // Total attractions should be positive for parks with attractions
                    expect(typeof stats.totalAttractions).toBe('number');
                    expect(stats.totalAttractions).toBeGreaterThanOrEqual(0);
                });
        });

        it('should return park with crowdForecast array (even if empty)', () => {
            return request(app.getHttpServer())
                .get('/v1/parks/europe/germany/bruehl/phantasialand')
                .expect(200)
                .expect((res) => {
                    expect(res.body).toBeDefined();
                    expect(res.body.crowdForecast).toBeDefined();
                    expect(Array.isArray(res.body.crowdForecast)).toBe(true);
                });
        });

        it('should return park with attractions array', () => {
            return request(app.getHttpServer())
                .get('/v1/parks/europe/germany/bruehl/phantasialand')
                .expect(200)
                .expect((res) => {
                    expect(res.body).toBeDefined();
                    expect(res.body.attractions).toBeDefined();
                    expect(Array.isArray(res.body.attractions)).toBe(true);
                    // Phantasialand should have attractions
                    expect(res.body.attractions.length).toBeGreaterThan(0);
                });
        });
    });

    describe('GET /v1/discovery/nearby', () => {
        it('should return parks with valid attraction counts', () => {
            // Phantasialand coordinates
            return request(app.getHttpServer())
                .get('/v1/discovery/nearby')
                .query({
                    lat: 50.7753,
                    lng: 6.0839,
                    radius: 10000,
                })
                .expect(200)
                .expect((res) => {
                    expect(res.body).toBeDefined();
                    expect(Array.isArray(res.body)).toBe(true);

                    if (res.body.length > 0) {
                        const park = res.body.find((p: any) => p.slug === 'phantasialand');

                        if (park) {
                            expect(park.analytics).toBeDefined();
                            expect(park.analytics.statistics).toBeDefined();

                            const stats = park.analytics.statistics;
                            expect(stats.totalAttractions).toBeDefined();
                            expect(typeof stats.totalAttractions).toBe('number');
                            expect(stats.totalAttractions).toBeGreaterThan(0);

                            expect(stats.operatingAttractions).toBeDefined();
                            expect(typeof stats.operatingAttractions).toBe('number');
                            expect(stats.operatingAttractions).toBeGreaterThanOrEqual(0);
                            expect(stats.operatingAttractions).toBeLessThanOrEqual(stats.totalAttractions);
                        }
                    }
                });
        });
    });
});
