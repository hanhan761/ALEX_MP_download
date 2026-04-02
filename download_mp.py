import os
import csv
import time
from mp_api.client import MPRester
from pymatgen.io.cif import CifWriter
from concurrent.futures import ProcessPoolExecutor, as_completed

API_KEY = "a4MAo1sY7OooRt51cILsXSxsy1i5ekw4"

def process_chunk(kwargs):
    chunk_ids = kwargs["chunk_ids"]
    output_dir = kwargs["output_dir"]
    
    results = []
    # Avoid output clutter from tqdm by using standard logging or suppressing warnings
    with MPRester(API_KEY, use_document_model=False) as mpr:
        try:
            # We fetch as a raw dict to save memory
            docs = mpr.materials.summary.search(
                material_ids=chunk_ids,
                fields=["material_id", "band_gap", "structure"]
            )
            
            for doc in docs:
                mat_id = str(doc["material_id"])
                bg = doc.get("band_gap")
                struct = doc.get("structure")
                
                if struct:
                    cif_path = os.path.join(output_dir, "cifs", f"{mat_id}.cif")
                    try:
                        # Write CIF
                        CifWriter(struct).write_file(cif_path)
                        results.append((mat_id, bg))
                    except Exception as e:
                        print(f"Failed to write CIF for {mat_id}: {e}")
        except Exception as e:
            print(f"Chunk failed: {e}")
            # If server error, maybe wait and retry once
            time.sleep(2)
            try:
                docs = mpr.materials.summary.search(
                    material_ids=chunk_ids,
                    fields=["material_id", "band_gap", "structure"]
                )
                for doc in docs:
                    mat_id = str(doc["material_id"])
                    bg = doc.get("band_gap")
                    struct = doc.get("structure")
                    if struct:
                        cif_path = os.path.join(output_dir, "cifs", f"{mat_id}.cif")
                        CifWriter(struct).write_file(cif_path)
                        results.append((mat_id, bg))
            except Exception as retry_e:
                print(f"Retry failed: {retry_e}")

    return results

def main():
    output_dir = "MP_Data"
    os.makedirs(os.path.join(output_dir, "cifs"), exist_ok=True)
    
    print("Initializing MPRester to fetch IDs...")
    with MPRester(API_KEY, use_document_model=False) as mpr:
        # Querying all materials that have a band_gap (includes metals where band_gap=0)
        print("Querying all materials with a band_gap (including metals)...")
        docs = mpr.materials.summary.search(band_gap=(0, None), fields=["material_id", "band_gap"])
        
        all_ids = []
        for d in docs:
            bg = d.get("band_gap")
            if bg is not None and bg >= 0:
                all_ids.append(str(d["material_id"]))

        print(f"Found {len(all_ids)} materials with a band_gap >= 0.")
        
    # We will chunk into 5000 IDs per worker task
    chunk_size = 2000
    chunks = [all_ids[i:i + chunk_size] for i in range(0, len(all_ids), chunk_size)]
    
    print(f"Split into {len(chunks)} chunks of size {chunk_size} for parallel processing.")
    
    tasks = [{"chunk_ids": chunk, "output_dir": output_dir} for chunk in chunks]
    
    csv_path = os.path.join(output_dir, "band_gaps.csv")
    
    # Check if we already have progress
    existing_ids = set()
    if os.path.exists(csv_path):
        with open(csv_path, "r", newline='') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row: existing_ids.add(row[0])
    
    csv_file = open(csv_path, "a", newline='')
    writer = csv.writer(csv_file)
    if len(existing_ids) == 0:
        writer.writerow(["mp_id", "band_gap"])
    
    # Number of workers depends on CPU. Let's use up to 8 max to avoid hitting API rate limits too hard.
    max_workers = 8 
    print(f"Starting ProcessPoolExecutor with {max_workers} workers...")
    
    completed = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk, task) for task in tasks]
        
        for future in as_completed(futures):
            chunk_results = future.result()
            new_rows = 0
            for row in chunk_results:
                if row[0] not in existing_ids:
                    writer.writerow(row)
                    existing_ids.add(row[0])
                    new_rows += 1
                    
            csv_file.flush()
            completed += 1
            print(f"Processed chunk {completed}/{len(chunks)}, retrieved {new_rows} new records.")
            
    csv_file.close()
    print("Download complete!")

if __name__ == "__main__":
    main()
