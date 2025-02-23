import osmium
import csv
import os


script_dir = os.getcwd()


def extract_pois_from_osm(osm_file, output_csv):
    """Extract POIs from OSM file and save them to JSON and CSV."""

    class POIHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.pois = []

        def node(self, n):
            """Process each OSM node and filter POIs."""
            if not n.location.valid():
                return

            lat, lon = n.location.lat, n.location.lon

            # Relevant POI categories
            poi_tags = ["amenity", "shop", "tourism", "public_transport", "leisure", "railway", "highway"]

            for tag in poi_tags:
                if tag in n.tags:
                    poi = {
                        "id": n.id,
                        "lat": lat,
                        "lon": lon,
                        "name": n.tags.get("name", "Unknown"),
                        "category": tag,
                        "type": n.tags[tag]
                    }
                    self.pois.append(poi)
                    break  # Only store the first matching POI type

        def save_to_csv(self, filename):
            """Save the extracted POIs to a CSV file."""
            with open(filename, "w", newline='', encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "lat", "lon", "name", "category", "type"])
                writer.writeheader()
                writer.writerows(self.pois)
            print(f"Saved {len(self.pois)} POIs to {filename}")

    # Create POI handler and process the OSM file
    handler = POIHandler()
    handler.apply_file(osm_file, locations=True)

    handler.save_to_csv(output_csv)


if __name__ == "__main__":
    osm_file_path = script_dir + "/data/poi_dataset/UK/"
    # state_list = ['AZ', 'DE', 'ID', 'IL', 'IN', 'LA']
    # state_list = ['MO', 'NJ', 'NV', 'PA', 'TN']
    state_list = ['XGL']

    for state in state_list :
        osm_path = osm_file_path + state
        for file in os.listdir(osm_path):
            if file.endswith('.pbf') :
                state_data = osm_path + '/' + file
        extract_pois_from_osm(state_data,osm_path+f"/{state}.csv")
        print(f"{state} POIs extracted! ")

    print("All POI DATA extracted!")


