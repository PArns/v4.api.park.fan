import { ThemeParksMapper } from "./themeparks.mapper";
import { EntityResponse, EntityType } from "./themeparks.types";

describe("ThemeParksMapper.mapRestaurant", () => {
  const restaurant: EntityResponse = {
    id: "wiki-restaurant-1",
    name: "Captain's Kitchen",
    entityType: EntityType.RESTAURANT,
    slug: null,
  };

  it("leaves requiresReservation unknown, because the source never says", () => {
    const mapped = new ThemeParksMapper().mapRestaurant(restaurant, "park-1");
    expect(mapped.requiresReservation).toBeNull();
  });

  it("takes cuisines only when the source sends them", () => {
    const mapper = new ThemeParksMapper();
    expect(mapper.mapRestaurant(restaurant, "park-1").cuisines).toBeUndefined();
    expect(
      mapper.mapRestaurant(
        { ...restaurant, cuisines: ["Italian", "Pizza"] },
        "park-1",
      ).cuisineType,
    ).toBe("Italian, Pizza");
  });
});
